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

from typing import Any

import httpx2
import pytest
from ghga_service_commons.api.mock_router import MockRouter

from ghga_connector.core.client import get_ratelimiting_retry_transport

__all__ = ["mock_health_checks", "mock_router"]


@pytest.fixture()
def mock_router(monkeypatch) -> MockRouter:
    """Serve every call made through `async_client` from a `MockRouter`.

    The router's transport is mounted as the innermost layer, so requests still pass
    through the real retry and rate limiting transports first, just like in
    `ghga_connector.core.client.async_client`.

    Tests register the endpoints they need, e.g.:
    ```
    @mock_router.get("/work-packages/{package_id}")
    def get_work_package(package_id: str) -> httpx2.Response:
        return httpx2.Response(200, json={"files": {}})
    ```
    A request that matches no registered endpoint raises an `HttpException` instead of
    being answered, so unexpected calls fail the test rather than passing silently.
    """
    router: MockRouter = MockRouter()

    def mock_mounts(config, base_transport=None, limits=None):
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


def mock_health_checks(monkeypatch, *, reachable: bool = True) -> None:
    """Answer the health checks performed by `is_service_healthy`.

    Those are made with a module level `httpx2.get` call rather than the client built
    by `async_client`, so they cannot be routed through the client's transport and
    `httpx2.get` itself has to be replaced.
    """
    router: MockRouter = MockRouter()

    @router.get(".*/health")
    def health(request: httpx2.Request) -> httpx2.Response:
        """Report the service as reachable or refuse the connection."""
        if not reachable:
            raise httpx2.ConnectError("mocked connection failure", request=request)
        return httpx2.Response(200, json={"status": "OK"})

    transport = router.as_transport()

    def mock_get(*, url: str, timeout: Any) -> httpx2.Response:
        """Stand in for `httpx2.get`, using the health check router as transport."""
        with httpx2.Client(transport=transport) as client:
            return client.get(url, timeout=timeout)

    monkeypatch.setattr(httpx2, "get", mock_get)
