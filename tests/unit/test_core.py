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

"""Tests for the core functions of the cli"""

from typing import Any

import httpx2
import pytest
from ghga_service_commons.api.mock_router import MockRouter

from ghga_connector.core.api_calls import is_service_healthy


@pytest.fixture()
def mock_health_endpoint(monkeypatch):
    """Serve https://ghga.de/health and refuse every other connection.

    `is_service_healthy` makes a module level `httpx2.get` call, so that call is what
    gets replaced here, leaving the URL handling and response parsing under test.
    """
    router: MockRouter = MockRouter()

    @router.get("https://ghga.de/health")
    def health() -> httpx2.Response:
        """Report GHGA as healthy."""
        return httpx2.Response(200, json={"status": "OK"})

    transport = router.as_transport()

    def mock_get(*, url: str, timeout: Any) -> httpx2.Response:
        if not url.startswith("https://ghga.de"):
            raise httpx2.ConnectError("mocked connection failure")
        with httpx2.Client(transport=transport) as client:
            return client.get(url, timeout=timeout)

    monkeypatch.setattr(httpx2, "get", mock_get)


@pytest.mark.parametrize(
    "api_url,timeout_in_seconds,expected_response",
    [
        ("https://bad_url", 5, False),
        ("https://ghga.de", 5, True),
        ("https://ghga.de/", 5, True),
        ("https://ghga.de/health", 5, True),
        ("https://ghga.de/health/", 5, True),
    ],
)
def test_is_service_healthy(
    api_url: str,
    timeout_in_seconds: int,
    expected_response: bool,
    mock_health_endpoint,
):
    """Test healthy check function"""
    response = is_service_healthy(api_url, timeout_in_seconds=timeout_in_seconds)
    assert response == expected_response
