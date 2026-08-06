# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Serve connector HTTP calls from a `MockRouter` instead of the network."""

import re
from typing import Any

import httpx2
import pytest
from ghga_service_commons.api.mock_router import MockRouter

from ghga_connector.core.client import get_ratelimiting_retry_transport
from tests.fixtures.config import get_test_config

__all__ = [
    "api_url",
    "mock_health_checks",
    "mock_router",
    "serve_httpx2_get_from",
]


def api_url(base_url: str, path: str) -> str:
    """Build a `MockRouter` pattern for `path` as served by the API at `base_url`.

    `MockRouter` matches its patterns against the whole request URL, and `mock_router`
    serves every host from a single router, so the API URL has to be part of the
    pattern. Without it, the pattern would just as happily match the same path served
    by a different API.
    """
    return re.escape(base_url) + path


@pytest.fixture()
def mock_router(monkeypatch) -> MockRouter:
    """Serve every call made through `async_client` from a `MockRouter`.

    The router's transport is mounted as the innermost layer, so requests still pass
    through the real retry and rate limiting transports first, just like in
    `ghga_connector.core.client.async_client`.

    Tests register the endpoints they need, anchoring each path to the API that serves
    it with `api_url`, e.g.:
    ```
    @mock_router.get(api_url(get_work_package_api_url(), "/work-packages/{package_id}"))
    def get_work_package(package_id: str) -> httpx2.Response:
        return httpx2.Response(200, json={"files": {}})
    ```
    A request that matches no registered endpoint raises an `HttpException` instead of
    being answered, so unexpected calls fail the test rather than passing silently.
    """
    # Mocked responses pass through the real retry transport, so without the test
    # config's `client_num_retries=0` every mocked 5xx would cost a real backoff sleep.
    monkeypatch.setattr("ghga_connector.config.CONFIG", get_test_config())
    router: MockRouter = MockRouter()

    def mock_mounts(config, limits=None):
        """Stand in for `ratelimiting_retry_proxies` and capture all traffic."""
        return {
            "all://": get_ratelimiting_retry_transport(
                base_transport=router.as_transport(), limits=limits
            )
        }

    monkeypatch.setattr(
        "ghga_connector.core.client.ratelimiting_retry_proxies", mock_mounts
    )
    return router


def serve_httpx2_get_from(monkeypatch, router: MockRouter) -> None:
    """Answer module level `httpx2.get` calls from `router`.

    `is_service_healthy` checks health endpoints with a module level `httpx2.get` call
    rather than the client built by `async_client`, so those calls cannot be routed
    through the client's transport and `httpx2.get` itself has to be replaced.
    """
    transport = router.as_transport()

    def mock_get(*, url: str, timeout: Any) -> httpx2.Response:
        """Stand in for `httpx2.get`, using the given router as transport."""
        with httpx2.Client(transport=transport) as client:
            return client.get(url, timeout=timeout)

    monkeypatch.setattr(httpx2, "get", mock_get)


def mock_health_checks(
    monkeypatch, *, reachable: bool = True, healthy_url: str = ".*"
) -> None:
    """Report the services the connector health checks as reachable or unreachable.

    `healthy_url` is a regex for the API URLs to report as healthy, defaulting to all of
    them. Pass `re.escape(...)` of a single API URL to pin down which URL the connector
    derives its health endpoint from; anything else then refuses the connection.
    """
    router: MockRouter = MockRouter()

    if reachable:

        @router.get(f"{healthy_url}/health")
        def health() -> httpx2.Response:
            """Report the service as reachable."""
            return httpx2.Response(200, json={"status": "OK"})

    # Endpoints are matched in registration order, so this only catches what is left.
    @router.get(".*")
    def unreachable(request: httpx2.Request) -> httpx2.Response:
        """Refuse to connect to any URL not reported as healthy."""
        raise httpx2.ConnectError("mocked connection failure", request=request)

    serve_httpx2_get_from(monkeypatch, router)
