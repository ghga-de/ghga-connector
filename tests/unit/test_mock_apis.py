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

"""Tests for the mock API harness itself"""

from unittest.mock import patch

import pytest

from ghga_connector.config import get_config
from ghga_connector.core.client import async_client
from tests.fixtures.config import get_test_config
from tests.fixtures.mock_api.joint import (
    MockApis,
    mock_apis,  # noqa: F401
)
from tests.fixtures.mock_api.router import OffLimitsError

pytestmark = [pytest.mark.asyncio]


@pytest.fixture(scope="function", autouse=True)
def apply_test_config():
    """Apply default test config"""
    with patch("ghga_connector.config.CONFIG", get_test_config()):
        yield


async def test_requests_to_the_internet_are_refused(
    mock_apis: MockApis,  # noqa: F811
):
    """Make sure a call the mocks don't cover cannot leave the test suite.

    The connector ships a live GHGA URL as its `wkvs_api_url` default, so a test that
    failed to apply the test config would otherwise call production for real.
    """
    # `Config` is a factory, so reach the model class through an instance
    live_default = type(get_test_config()).model_fields["wkvs_api_url"].default
    assert live_default.startswith("https://"), "expected a real URL as the default"
    assert "127.0.0.1" not in live_default

    async with async_client() as client:
        with pytest.raises(OffLimitsError, match=live_default):
            await client.get(f"{live_default}/values")


async def test_mocked_apis_are_still_reachable(
    mock_apis: MockApis,  # noqa: F811
):
    """Make sure refusing the internet doesn't also refuse the mocks."""
    async with async_client() as client:
        response = await client.get(f"{get_config().wkvs_api_url}/values")

    assert response.status_code == 200
    assert response.json()["dcs_api_url"]
    assert mock_apis.wkvs.requests
