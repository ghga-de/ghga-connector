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

"""Unit tests for the HTTP client for the Upload API"""

import json
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx2
import pytest
import pytest_asyncio
from ghga_service_commons.api.mock_router import MockRouter
from httpx2 import Response
from pydantic import UUID4
from tenacity import RetryError

from ghga_connector import exceptions
from ghga_connector.constants import UPLOAD_LISTING_PAGE_SIZE
from ghga_connector.core.client import async_client
from ghga_connector.core.uploading.api_calls import (
    UploadClient,
    _check_for_request_errors,
)
from tests.fixtures import set_runtime_test_config  # noqa: F401
from tests.fixtures.mock_api.router import mock_router  # noqa: F401
from tests.fixtures.utils import TEST_FUB_ID, TEST_RDUB_ID, TEST_STORAGE_ALIAS1

pytestmark = [pytest.mark.asyncio]

FILE_ID = UUID("550e8400-e29b-41d4-a716-446655440002")
FILE_ALIAS = "test-file-1"
UPLOAD_URL = "http://upload_url"

# The checksums announced when completing an upload, which are also the request body
CHECKSUMS: dict[str, Any] = {
    "decrypted_sha256": "abc123",
    "encrypted_md5": "xyz456",
    "encrypted_parts_md5": ["part1_md5"],
    "encrypted_parts_sha256": ["part1_sha256"],
}

# The paths the Upload API serves, relative to the configured Upload API URL
UPLOADS_PATH = "/boxes/{box_id}/uploads"
UPLOAD_PATH = f"{UPLOADS_PATH}/{{file_id}}"
PART_PATH = f"{UPLOAD_PATH}/parts/{{part_no}}"


@pytest_asyncio.fixture()
async def upload_client(
    set_runtime_test_config,  # noqa: F811
    mock_router: MockRouter,  # noqa: F811
    monkeypatch,
) -> AsyncGenerator[UploadClient, None]:
    """Create a configured UploadClient.

    The WPAT user input is patched, and the Upload API health
    check is also patched to always return True.
    """
    box_ids_mock = AsyncMock()
    box_ids_mock.return_value = (TEST_RDUB_ID, TEST_FUB_ID)
    monkeypatch.setattr(
        "ghga_connector.core.work_package.WorkPackageClient.get_package_box_ids",
        box_ids_mock,
    )

    # Mock the health endpoint
    monkeypatch.setattr(
        "ghga_connector.core.uploading.api_calls.is_service_healthy", lambda s: True
    )

    mock_work_package_client = AsyncMock()
    mock_work_package_client.get_upload_wot.return_value = "wot"
    mock_work_package_client.get_package_box_ids.return_value = (
        TEST_RDUB_ID,
        TEST_FUB_ID,
    )
    async with async_client() as client:
        yield UploadClient(client=client, work_package_client=mock_work_package_client)


async def test_create_file_upload_success(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that create_file_upload posts the correct body and returns the file ID."""
    decrypted_size = 20 * 1024**3
    encrypted_size = 20 * 1024**3 + 2000  # larger due to encryption padding & envelope
    expected_body = {
        "alias": FILE_ALIAS,
        "decrypted_size": decrypted_size,
        "encrypted_size": encrypted_size,
        "part_size": 100,
        "overwrite": False,
    }

    @mock_router.post(UPLOADS_PATH)
    def create_file_upload(box_id: str, request: httpx2.Request) -> httpx2.Response:
        """Create the file upload, but only for the expected request body."""
        assert box_id == str(TEST_FUB_ID)
        assert json.loads(request.read()) == expected_body
        return httpx2.Response(
            201,
            json={
                "file_id": str(FILE_ID),
                "alias": FILE_ALIAS,
                "storage_alias": TEST_STORAGE_ALIAS1,
            },
        )

    file_id, storage_alias = await upload_client.create_file_upload(
        file_alias=FILE_ALIAS,
        decrypted_size=decrypted_size,
        encrypted_size=encrypted_size,
        part_size=100,
    )
    assert file_id == FILE_ID
    assert storage_alias == TEST_STORAGE_ALIAS1

    # Check that we get the right type of WOT
    upload_client._work_package_client.get_upload_wot.assert_called_with(  # type: ignore
        work_type="create",
        research_data_upload_box_id=TEST_RDUB_ID,
        file_id=None,
        alias=FILE_ALIAS,
    )


async def test_create_file_upload_error(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that an unsuccessful status code triggers the error translation."""

    @mock_router.post(UPLOADS_PATH)
    def create_file_upload(box_id: str) -> httpx2.Response:
        """Refuse to create the file upload."""
        return httpx2.Response(500)

    with pytest.raises(exceptions.UnexpectedError):
        _ = await upload_client.create_file_upload(
            file_alias=FILE_ALIAS,
            decrypted_size=2000,
            encrypted_size=2124,
            part_size=100,
        )


@pytest.mark.parametrize("overwrite", [True, False])
async def test_create_file_upload_sends_overwrite(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
    overwrite: bool,
):
    """Make sure create_file_upload forwards the overwrite flag in the request body."""
    decrypted_size = 2000
    encrypted_size = 2124
    expected_body = {
        "alias": FILE_ALIAS,
        "decrypted_size": decrypted_size,
        "encrypted_size": encrypted_size,
        "part_size": 100,
        "overwrite": overwrite,
    }

    @mock_router.post(UPLOADS_PATH)
    def create_file_upload(box_id: str, request: httpx2.Request) -> httpx2.Response:
        """Create the file upload, checking the forwarded overwrite flag."""
        assert json.loads(request.read()) == expected_body
        return httpx2.Response(
            201,
            json={
                "file_id": str(FILE_ID),
                "alias": FILE_ALIAS,
                "storage_alias": TEST_STORAGE_ALIAS1,
            },
        )

    file_id, _ = await upload_client.create_file_upload(
        file_alias=FILE_ALIAS,
        decrypted_size=decrypted_size,
        encrypted_size=encrypted_size,
        part_size=100,
        overwrite=overwrite,
    )
    assert file_id == FILE_ID


async def test_get_box_uploads(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that get_box_uploads requests a view WOT and parses the listing."""
    response_body = {
        "items": [
            {
                "id": str(FILE_ID),
                "alias": FILE_ALIAS,
                "decrypted_size": 2048,
                "encrypted_size": 4096,
                "state": "inbox",
                # An unexpected extra field should be ignored, not cause a failure
                "some_unmodeled_field": "ignored",
            }
        ],
        "total_count": 1,
    }

    # The listing endpoint is queried with pagination parameters, so the registered
    # path has to tolerate a query string.
    @mock_router.get(f"{UPLOADS_PATH}.*")
    def get_box_uploads(box_id: str, request: httpx2.Request) -> httpx2.Response:
        """Return the single page of the listing."""
        assert box_id == str(TEST_FUB_ID)
        assert request.url.params["skip"] == "0"
        assert request.url.params["limit"] == str(UPLOAD_LISTING_PAGE_SIZE)
        return httpx2.Response(200, json=response_body)

    uploads = await upload_client.get_box_uploads()

    assert len(uploads) == 1
    assert uploads[0].file_id == FILE_ID
    assert uploads[0].alias == FILE_ALIAS
    assert uploads[0].decrypted_size == 2048
    assert uploads[0].state == "inbox"

    # Check that we request a "view" WOT for the box
    upload_client._work_package_client.get_upload_wot.assert_called_with(  # type: ignore
        work_type="view",
        research_data_upload_box_id=TEST_RDUB_ID,
        file_id=None,
        alias=None,
    )


async def test_get_box_uploads_error(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that an unsuccessful status code triggers the error translation."""

    @mock_router.get(f"{UPLOADS_PATH}.*")
    def get_box_uploads(box_id: str) -> httpx2.Response:
        """Refuse to list the box contents."""
        return httpx2.Response(500)

    with pytest.raises(exceptions.UnexpectedError):
        _ = await upload_client.get_box_uploads()


async def test_get_box_uploads_pagination(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that get_box_uploads fetches every page of a paginated listing."""
    total_count = UPLOAD_LISTING_PAGE_SIZE + 1

    def _item(index: int) -> dict[str, Any]:
        return {
            "id": str(uuid4()),
            "alias": f"file-{index}",
            "decrypted_size": 2048,
            "encrypted_size": 4096,
            "state": "inbox",
        }

    @mock_router.get(f"{UPLOADS_PATH}.*")
    def get_box_uploads(box_id: str, request: httpx2.Request) -> httpx2.Response:
        """Serve a full first page, then a second page with the remaining item."""
        assert request.url.params["limit"] == str(UPLOAD_LISTING_PAGE_SIZE)
        skip = int(request.url.params["skip"])
        items = (
            [_item(index) for index in range(UPLOAD_LISTING_PAGE_SIZE)]
            if skip == 0
            else [_item(UPLOAD_LISTING_PAGE_SIZE)]
        )
        return httpx2.Response(200, json={"items": items, "total_count": total_count})

    uploads = await upload_client.get_box_uploads()
    assert len(uploads) == total_count
    assert {upload.alias for upload in uploads} == {
        f"file-{i}" for i in range(total_count)
    }


async def test_get_part_upload_url(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that get_part_upload_url returns the presigned URL from the API."""

    @mock_router.get(PART_PATH)
    def get_part_upload_url(box_id: str, file_id: str, part_no: int) -> httpx2.Response:
        """Hand out the presigned upload URL for the requested part."""
        assert (box_id, file_id, part_no) == (str(TEST_FUB_ID), str(FILE_ID), 1)
        return httpx2.Response(200, json=UPLOAD_URL)

    upload_url = await upload_client.get_part_upload_url(file_id=FILE_ID, part_no=1)
    assert upload_url == UPLOAD_URL

    # Check that we get the right type of WOT
    upload_client._work_package_client.get_upload_wot.assert_called_with(  # type: ignore
        work_type="upload",
        research_data_upload_box_id=TEST_RDUB_ID,
        file_id=FILE_ID,
        alias=None,
    )


async def test_get_part_upload_url_error(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that an unsuccessful status code triggers the error translation."""

    @mock_router.get(PART_PATH)
    def get_part_upload_url(box_id: str, file_id: str, part_no: int) -> httpx2.Response:
        """Refuse to hand out an upload URL."""
        return httpx2.Response(500)

    with pytest.raises(exceptions.UnexpectedError):
        _ = await upload_client.get_part_upload_url(file_id=FILE_ID, part_no=1)


async def test_upload_file_part(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that upload_file_part fetches the presigned URL and PUTs the content to S3."""

    @mock_router.get(PART_PATH)
    def get_part_upload_url(box_id: str, file_id: str, part_no: int) -> httpx2.Response:
        """Hand out the presigned upload URL for the requested part."""
        return httpx2.Response(200, json=UPLOAD_URL)

    @mock_router.put(f"{UPLOAD_URL}/?")
    def upload_part(request: httpx2.Request) -> httpx2.Response:
        """Accept the part content at the presigned URL."""
        assert request.read() == b"abc123"
        return httpx2.Response(200)

    await upload_client.upload_file_part(file_id=FILE_ID, content=b"abc123", part_no=1)


async def test_complete_file_upload(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that complete_file_upload sends the correct checksums in the PATCH request."""

    @mock_router.patch(UPLOAD_PATH)
    def complete_file_upload(
        box_id: str, file_id: str, request: httpx2.Request
    ) -> httpx2.Response:
        """Complete the upload, but only for the expected checksums."""
        assert (box_id, file_id) == (str(TEST_FUB_ID), str(FILE_ID))
        assert json.loads(request.read()) == CHECKSUMS
        return httpx2.Response(204)

    await upload_client.complete_file_upload(
        file_id=FILE_ID, file_alias=FILE_ALIAS, **CHECKSUMS
    )

    # Check that we get the right type of WOT
    upload_client._work_package_client.get_upload_wot.assert_called_with(  # type: ignore
        work_type="close",
        research_data_upload_box_id=TEST_RDUB_ID,
        file_id=FILE_ID,
        alias=None,
    )


async def test_complete_file_upload_error(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that an unsuccessful status code triggers the error translation."""

    @mock_router.patch(UPLOAD_PATH)
    def complete_file_upload(box_id: str, file_id: str) -> httpx2.Response:
        """Refuse to complete the upload."""
        return httpx2.Response(500)

    with pytest.raises(exceptions.UnexpectedError):
        await upload_client.complete_file_upload(
            file_id=FILE_ID, file_alias=FILE_ALIAS, **CHECKSUMS
        )


async def test_delete_file(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that delete_file sends a DELETE request and uses the correct work order token."""

    @mock_router.delete(UPLOAD_PATH)
    def delete_file(box_id: str, file_id: str) -> httpx2.Response:
        """Delete the file upload."""
        assert (box_id, file_id) == (str(TEST_FUB_ID), str(FILE_ID))
        return httpx2.Response(204)

    await upload_client.delete_file(file_id=FILE_ID, file_alias=FILE_ALIAS)

    # Check that we get the right type of WOT
    upload_client._work_package_client.get_upload_wot.assert_called_with(  # type: ignore
        work_type="delete",
        research_data_upload_box_id=TEST_RDUB_ID,
        file_id=FILE_ID,
        alias=None,
    )


async def test_delete_file_error(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that an unsuccessful status code triggers the error translation."""

    @mock_router.delete(UPLOAD_PATH)
    def delete_file(box_id: str, file_id: str) -> httpx2.Response:
        """Refuse to delete the file upload."""
        return httpx2.Response(500)

    with pytest.raises(exceptions.UnexpectedError):
        await upload_client.delete_file(file_id=FILE_ID, file_alias=FILE_ALIAS)


async def test_delete_file_not_in_box(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Test that a "fileUploadNotFound" 404 means the file is no longer in the box."""

    @mock_router.delete(UPLOAD_PATH)
    def delete_file(box_id: str, file_id: str) -> httpx2.Response:
        """Report the file upload as unknown."""
        return httpx2.Response(404, json={"exception_id": "fileUploadNotFound"})

    with pytest.raises(exceptions.FileNotInBoxError):
        await upload_client.delete_file(file_id=FILE_ID, file_alias=FILE_ALIAS)


@pytest.mark.parametrize(
    "status_code, response_json, file_upload_box_id, file_alias, file_id, expected_error",
    [
        # 400 status code - noSuchStorage
        (
            400,
            {"exception_id": "noSuchStorage"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.S3StorageError,
        ),
        # 400 status code - checksumMismatch
        (
            400,
            {"exception_id": "checksumMismatch"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.ChecksumMismatchError,
        ),
        # 400 status code - invalidPartSize
        (
            400,
            {"exception_id": "invalidPartSize"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.InvalidPartSize,
        ),
        # 400 status code - uploadSizeMismatch
        (
            400,
            {"exception_id": "uploadSizeMismatch"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UploadSizeMismatchError,
        ),
        # 400 status code - no matching exception id
        (
            400,
            {"exception_id": "nosuchexceptionid"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UnexpectedError,
        ),
        # 401 status code
        (
            401,
            {"exception_id": "authorizationError"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.AuthorizationError,
        ),
        # 403 status code
        (
            403,
            {"exception_id": "authorizationError"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.AuthorizationError,
        ),
        # 404 status codes - boxNotFound
        (
            404,
            {"exception_id": "boxNotFound"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.InvalidBoxError,
        ),
        # 404 status codes - fileUploadNotFound, alias known -> alias-based error
        (
            404,
            {"exception_id": "fileUploadNotFound"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.FileNotInBoxError,
        ),
        # 404 status codes - s3UploadNotFound
        (
            404,
            {"exception_id": "s3UploadNotFound"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.S3UploadMissingError,
        ),
        # 404 status codes - no matching exception id
        (
            404,
            {"exception_id": "nosuchexceptionid"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UnexpectedError,
        ),
        # 409 status codes - boxStateError
        (
            409,
            {"exception_id": "boxStateError"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UploadBoxLockedError,
        ),
        # 409 status codes - fileUploadAlreadyExists
        (
            409,
            {"exception_id": "fileUploadAlreadyExists"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UploadAlreadyExistsError,
        ),
        # 409 status codes - orphanedMultipartUpload
        (
            409,
            {"exception_id": "orphanedMultipartUpload"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.OrphanedUploadError,
        ),
        # 409 status codes - fileUploadStateError
        (
            409,
            {"exception_id": "fileUploadStateError"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.FileUploadStateError,
        ),
        # 507 status code - boxMaxSizeExceeded
        (
            507,
            {"exception_id": "boxMaxSizeExceeded"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UploadBoxSizeExceededError,
        ),
        # 429 status code
        (
            429,
            {"exception_id": "tooManyOpenUploads"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.TooManyRequestsError,
        ),
        # 507 status code - no matching exception id
        (
            507,
            {"exception_id": "nosuchexceptionid"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UnexpectedError,
        ),
        # 400 status codes - no matching exception id
        (
            400,
            {"exception_id": "nosuchexceptionid"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UnexpectedError,
        ),
        # Unexpected status code
        (
            500,
            {"exception_id": "internalServerError"},
            TEST_FUB_ID,
            FILE_ALIAS,
            FILE_ID,
            exceptions.UnexpectedError,
        ),
        # Test with None values for optional parameters - 404 boxNotFound
        (
            404,
            {"exception_id": "boxNotFound"},
            None,
            None,
            None,
            exceptions.InvalidBoxError,
        ),
        # Test with None values for optional parameters - 409 boxStateError
        (
            409,
            {"exception_id": "boxStateError"},
            None,
            None,
            None,
            exceptions.UploadBoxLockedError,
        ),
        # Test with partial None values - file_alias None, others present
        (
            404,
            {"exception_id": "fileUploadNotFound"},
            TEST_FUB_ID,
            None,
            FILE_ID,
            exceptions.InvalidFileUploadError,
        ),
        # Test with partial None values - file_id None, others present
        (
            409,
            {"exception_id": "orphanedMultipartUpload"},
            TEST_FUB_ID,
            FILE_ALIAS,
            None,
            exceptions.OrphanedUploadError,
        ),
    ],
)
async def test_handle_bad_status_codes(
    upload_client: UploadClient,
    status_code: int,
    response_json: dict[str, Any],
    file_upload_box_id: UUID4 | None,
    file_alias: str | None,
    file_id: UUID4 | None,
    expected_error: type[Exception],
):
    """Make sure _handle_bad_status_codes translates HTTP errors to the correct exception types."""
    response = Response(status_code=status_code, json=response_json)
    with pytest.raises(expected_error):
        upload_client._handle_bad_status_codes(
            status_code=status_code,
            response=response,
            file_upload_box_id=file_upload_box_id,
            file_alias=file_alias,
            file_id=file_id,
        )


def make_retry_error(exception: Exception) -> RetryError:
    """Wrap an exception inside a tenacity RetryError via a mock last_attempt."""
    mock_attempt = MagicMock()
    mock_attempt.exception.return_value = exception
    return RetryError(last_attempt=mock_attempt)


async def test_check_for_request_errors_connect_error():
    """Make sure a RetryError wrapping ConnectError is translated to ConnectionFailedError."""
    retry_error = make_retry_error(httpx2.ConnectError("connection refused"))
    with pytest.raises(exceptions.ConnectionFailedError):
        _check_for_request_errors(retry_error, "http://example.com")


async def test_check_for_request_errors_connect_timeout():
    """Make sure a RetryError wrapping ConnectTimeout is translated to ConnectionFailedError."""
    retry_error = make_retry_error(httpx2.ConnectTimeout("timed out"))
    with pytest.raises(exceptions.ConnectionFailedError):
        _check_for_request_errors(retry_error, "http://example.com")


async def test_check_for_request_errors_other_request_error():
    """Make sure a RetryError wrapping a non-connect RequestError is translated to RequestFailedError."""
    retry_error = make_retry_error(httpx2.ReadTimeout("read timeout"))
    with pytest.raises(exceptions.RequestFailedError):
        _check_for_request_errors(retry_error, "http://example.com")


async def test_get_part_upload_url_first_403_triggers_cache_bust_and_second_403_raises(
    upload_client: UploadClient,
    mock_router: MockRouter,  # noqa: F811
):
    """Make sure a 403 on the first attempt triggers a bust_cache retry, and a 403 on that retry raises AuthorizationError."""

    # Return 403 on both attempts (first call and the bust_cache retry)
    @mock_router.get(PART_PATH)
    def get_part_upload_url(box_id: str, file_id: str, part_no: int) -> httpx2.Response:
        """Refuse to hand out an upload URL."""
        return httpx2.Response(403, json={"exception_id": "authorizationError"})

    # Replace the AsyncMock auto-attribute with a plain MagicMock so calling
    # cache_invalidate() doesn't create an unawaited coroutine warning.
    cache_invalidate_mock = MagicMock()
    upload_client._work_package_client.get_upload_wot.cache_invalidate = (  # type: ignore
        cache_invalidate_mock
    )

    with pytest.raises(exceptions.AuthorizationError):
        await upload_client.get_part_upload_url(file_id=FILE_ID, part_no=1)

    # The cache should have been invalidated exactly once (on the bust_cache=True retry)
    cache_invalidate_mock.assert_called_once()
