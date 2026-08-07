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

"""Every GHGA API the connector talks to, mocked at once.

This is what integration tests use: the connector bootstraps itself from the WKVS mock,
which points it at the other three, so a whole up- or download runs against them without
a single endpoint being registered by hand. Traffic to the S3 testcontainer still goes
out over the network, which is what makes the presigned URLs worth handing out.
"""

from dataclasses import dataclass

import pytest
from ghga_service_commons.api.mock_router import HttpException, MockRouter

from tests.fixtures.config import get_test_config
from tests.fixtures.mock_api.apis import (
    DownloadApiMock,
    UploadApiMock,
    WkvsMock,
    WorkPackageApiMock,
)
from tests.fixtures.mock_api.router import (
    httpyexpect_response,
    serve_mock_api_host_from,
)

__all__ = [
    "MockApis",
    "mock_apis",
]


@dataclass
class MockApis:
    """The mocked GHGA APIs, and the router serving all of them.

    Everything a test needs to arrange is a handler swap on one of the mocks; `router` is
    there for the rare endpoint no GHGA API serves.
    """

    router: MockRouter
    wkvs: WkvsMock
    work_package: WorkPackageApiMock
    download: DownloadApiMock
    upload: UploadApiMock


@pytest.fixture()
def mock_apis(monkeypatch) -> MockApis:
    """Serve every GHGA API from a mock, while letting S3 traffic reach the container.

    The config is left to the test's own `apply_test_config`, so that a test overriding a
    config value keeps it; only the WKVS URL is read here, to know where to serve it.

    A request no endpoint matches is answered with the 404 the `MockRouter` raises for it
    rather than that exception surfacing out of the transport, so the connector sees an
    error response from an unmocked call just as it did from the FastAPI mock app.
    """
    router: MockRouter[HttpException] = MockRouter(
        exception_handler=httpyexpect_response,
        exceptions_to_handle=(HttpException,),
    )
    mocks = MockApis(
        router=router,
        wkvs=WkvsMock(router, get_test_config().wkvs_api_url),
        work_package=WorkPackageApiMock(router),
        download=DownloadApiMock(router),
        upload=UploadApiMock(router),
    )
    serve_mock_api_host_from(monkeypatch, router)
    return mocks
