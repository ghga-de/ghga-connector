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

import ipaddress
import json
import re
from collections.abc import Awaitable, Callable
from email.utils import format_datetime
from typing import Any

import httpx2
import pytest
from ghga_service_commons.api.mock_router import HttpException, MockRouter
from ghga_service_commons.utils.utc_dates import now_as_utc

from ghga_connector.core.client import get_ratelimiting_retry_transport
from tests.fixtures.config import get_test_config

__all__ = [
    "LOOPBACK_HOSTS",
    "MOCK_API_HOST",
    "OffLimitsError",
    "ResponseHandler",
    "api_url",
    "caching_headers",
    "httpyexpect_error",
    "httpyexpect_response",
    "may_be_reached",
    "mock_health_checks",
    "mock_router",
    "respond",
    "serve_httpx2_get_from",
    "serve_mock_api_host_from",
    "serves_a_mocked_api",
]

# The host the mocked GHGA APIs are served from, and the other spellings of it they also
# answer to. Which one the tests are handed depends on how Docker is reached, so a URL
# naming any of them has to be recognized as pointing at the mocks.
MOCK_API_HOST = "127.0.0.1"
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")
_LOOPBACK_PATTERN = "(?:" + "|".join(re.escape(host) for host in LOOPBACK_HOSTS) + ")"

# A handler answers one request. It is passed the request and, as keyword arguments,
# the path variables of the endpoint it is registered on, so it can ignore either.
# Handlers may be `async`; `httpx2.MockTransport` awaits what they return.
ResponseHandler = Callable[..., httpx2.Response | Awaitable[httpx2.Response]]


def caching_headers(max_age: int) -> dict[str, str]:
    """Headers marking a response as cacheable by `hishel` for `max_age` seconds."""
    return {
        "Cache-Control": f"max-age={max_age}, private",
        "date": format_datetime(now_as_utc()),
    }


def respond(
    status_code: int,
    json: Any = None,
    *,
    headers: dict[str, str] | None = None,
    cache_for: int | None = None,
) -> ResponseHandler:
    """Make a handler that always answers with the same status code and JSON body.

    A `json` of `None` means no body at all. `cache_for` marks the response as cacheable
    for that many seconds, freshly dated on every answer.
    """

    def handler(request: httpx2.Request, **path_variables: Any) -> httpx2.Response:
        """Answer with the canned response."""
        response_headers = dict(headers or {})
        if cache_for is not None:
            response_headers |= caching_headers(cache_for)
        return httpx2.Response(status_code, json=json, headers=response_headers)

    return handler


def httpyexpect_error(
    status_code: int, exception_id: str, description: str, data: dict[str, Any]
) -> httpx2.Response:
    """The response a GHGA service sends for an error, in the httpyexpect schema.

    `data` is serialized leniently, because it does not always hold plain JSON: the 422
    `MockRouter` raises for an uncastable path variable reports a class as the type it
    tried to cast to.
    """
    body = {"exception_id": exception_id, "description": description, "data": data}
    return httpx2.Response(
        status_code,
        content=json.dumps(body, default=str),
        headers={"content-type": "application/json"},
    )


def httpyexpect_response(
    request: httpx2.Request, exception: HttpException
) -> httpx2.Response:
    """Answer with an `HttpException` rather than letting it propagate.

    `MockRouter` raises one when no endpoint matches a request, or when a path variable
    doesn't fit the type its endpoint declares. Turning it into a response, as
    `configure_exception_handler` did for the FastAPI mock app these mocks replaced,
    keeps the connector's own error translation in the loop for a call the mocks don't
    cover, instead of the exception surfacing straight out of the transport.
    """
    return httpyexpect_error(
        exception.status_code,
        exception.body.exception_id,
        exception.body.description,
        exception.body.data,
    )


def _host_of(base_url: str) -> str:
    """The host `base_url` names, whether or not it carries a scheme."""
    return httpx2.URL(base_url).host or base_url.split("/", 1)[0].split(":", 1)[0]


def api_url(base_url: str, path: str) -> str:
    """Build a `MockRouter` pattern for `path` as served by the API at `base_url`.

    `MockRouter` matches its patterns against the whole request URL and serves every host
    from a single router, so without the API URL in the pattern it would just as happily
    match the same path served by a different API.

    An API on the loopback interface is matched under any spelling of it, so that a call
    to `localhost` reaches the same mock as one to `127.0.0.1`.
    """
    pattern = re.escape(base_url)
    host = _host_of(base_url)
    if host in LOOPBACK_HOSTS:
        pattern = pattern.replace(re.escape(host), _LOOPBACK_PATTERN, 1)
    return pattern + path


@pytest.fixture()
def mock_router(monkeypatch) -> MockRouter:
    """Serve every call made through `async_client` from a `MockRouter`.

    The router's transport is mounted as the innermost layer, so requests still pass
    through the real retry and rate limiting transports first.

    Tests register the endpoints they need, anchoring each path to the API that serves it
    with `api_url`, e.g.:
    ```
    @mock_router.get(api_url(get_work_package_api_url(), "/work-packages/{package_id}"))
    def get_work_package(package_id: str) -> httpx2.Response:
        return httpx2.Response(200, json={"files": {}})
    ```
    A request matching no registered endpoint raises an `HttpException` instead of being
    answered, so unexpected calls fail the test rather than passing silently.
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


class OffLimitsError(RuntimeError):
    """Raised when a test is about to send a request out to the internet."""

    def __init__(self, url: httpx2.URL):
        """Name the request that was refused, and what to do about it."""
        super().__init__(
            f"A test tried to reach {url}, which is neither one of the mocked GHGA APIs"
            " nor anything else the test environment runs. Mock the API it belongs to"
            " rather than letting the request out."
        )


def serves_a_mocked_api(url: httpx2.URL) -> bool:
    """Whether `url` is addressed to one of the mocked GHGA APIs.

    They are served on the loopback interface under the default port for their scheme.
    Anything else on loopback is a container the test environment published on a port of
    its own - the S3 testcontainer, in practice.
    """
    return url.host in LOOPBACK_HOSTS and url.port is None


def may_be_reached(url: httpx2.URL) -> bool:
    """Whether a request to `url` may leave the test suite for the real network.

    Only what the test environment itself runs may be reached. Testcontainers reports a
    container's address as whatever the Docker host happens to be - loopback when Docker
    is local, `host.docker.internal` from inside a devcontainer, a private bridge address
    when neither - so all three pass, while the internet at large must not.
    """
    host = url.host
    if host in LOOPBACK_HOSTS or host.endswith(".internal"):
        return True
    try:
        return ipaddress.ip_address(host).is_private
    except ValueError:
        return False


class MockApiTransport(httpx2.AsyncBaseTransport):
    """Sends GHGA API calls to the mocks and refuses anything bound for the internet.

    Requests to whatever else the test environment runs - the S3 testcontainer, whose
    presigned URLs the mocked APIs hand out - go over the network as usual, since the
    point of those URLs is that they address real storage.
    """

    def __init__(
        self,
        router: MockRouter,
        *,
        base_transport: httpx2.AsyncBaseTransport | None = None,
        limits: httpx2.Limits | None = None,
    ) -> None:
        self._mocked = get_ratelimiting_retry_transport(
            base_transport=router.as_transport(), limits=limits
        )
        self._network = get_ratelimiting_retry_transport(
            base_transport=base_transport, limits=limits
        )

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        """Send the request wherever it is allowed to go, or refuse to send it."""
        if serves_a_mocked_api(request.url):
            return await self._mocked.handle_async_request(request)
        if may_be_reached(request.url):
            return await self._network.handle_async_request(request)
        raise OffLimitsError(request.url)


def serve_mock_api_host_from(monkeypatch, router: MockRouter) -> None:
    """Answer calls to the mocked GHGA APIs from `router`, letting local traffic out.

    Unlike `mock_router`, which swallows every request, this leaves the connector free to
    reach the S3 testcontainer, which integration tests need. Anything bound for the
    internet is refused instead of sent: the connector's default `wkvs_api_url` is a live
    GHGA URL, so a test that failed to apply the test config would call production.
    """

    def mock_mounts(config, base_transport=None, limits=None):
        """Stand in for `ratelimiting_retry_proxies`, sorting out where calls may go."""
        return {
            "all://": MockApiTransport(
                router, base_transport=base_transport, limits=limits
            )
        }

    monkeypatch.setattr(
        "ghga_connector.core.client.ratelimiting_retry_proxies", mock_mounts
    )


def serve_httpx2_get_from(monkeypatch, router: MockRouter) -> None:
    """Answer module level `httpx2.get` calls from `router`.

    `is_service_healthy` checks health endpoints with a module level `httpx2.get` rather
    than the client built by `async_client`, so those calls cannot be routed through the
    client's transport and `httpx2.get` itself has to be replaced.
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

    # `respond` cannot be used here: `MockRouter` reads the path variables an endpoint
    # wants off its signature, and the `**path_variables` it accepts for the API mocks
    # would not match any path.
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
