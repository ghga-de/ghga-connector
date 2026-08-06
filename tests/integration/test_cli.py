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

"""Tests for the up- and download functions of the cli"""

import os
import pathlib
from contextlib import nullcontext
from dataclasses import dataclass
from filecmp import cmp
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx2
import pytest
from ghga_service_commons.utils.utc_dates import now_as_utc

from ghga_connector import exceptions
from ghga_connector.constants import C4GH, DEFAULT_PART_SIZE
from ghga_connector.core.main import async_download
from tests.fixtures import state
from tests.fixtures.config import get_test_config
from tests.fixtures.mock_api.apis import DownloadApiMock, envelope_response
from tests.fixtures.mock_api.joint import (
    MockApis,
    mock_apis,  # noqa: F401
)
from tests.fixtures.mock_api.router import caching_headers, mock_health_checks
from tests.fixtures.s3 import (  # noqa: F401
    S3Fixture,
    get_big_s3_object,
    reset_state,
    s3_fixture,
)
from tests.fixtures.utils import (
    PRIVATE_KEY_FILE,
    PUBLIC_KEY_FILE,
    patch_work_package_functions,  # noqa: F401
)

GET_PACKAGE_FILES_ATTR = (
    "ghga_connector.core.work_package.WorkPackageClient.get_package_files"
)
FAKE_ENVELOPE = b"Thisisafakeenvelope"
SHORT_LIFESPAN = 10

# The file ID the Download API reports as not staged yet, and the work order tokens it
# refuses. Tests provoke the latter by patching what the connector decrypts a token to.
RETRY_FILE_ID = "retry"
AUTH_FAILURE_TOKEN = "authfail_normal"
FILE_ID_MISMATCH_TOKEN = "file_id_mismatch"

pytestmark = [pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(scope="function", autouse=True)
def apply_test_config():
    """Apply default test config"""
    with patch("ghga_connector.config.CONFIG", get_test_config()):
        yield


@dataclass
class StagedObject:
    """An object the Download API reports as ready, and the S3 object behind it."""

    file_id: str
    bucket_id: str
    size: int
    envelope: bytes | None = FAKE_ENVELOPE


def _refused_work_order_token(request: httpx2.Request) -> httpx2.Response | None:
    """Refuse the request if it carries one of the work order tokens tests provoke.

    A plain 403 explains itself in `detail`, an httpyexpect one in `description`. The
    connector reads whichever is there, so both flavors are exercised.
    """
    token = request.headers.get("authorization", "").removeprefix("Bearer ")
    if token == AUTH_FAILURE_TOKEN:
        return httpx2.Response(
            403, json={"detail": "This is not the token you're looking for."}
        )
    if token == FILE_ID_MISMATCH_TOKEN:
        return httpx2.Response(
            403,
            json={
                "exception_id": "wrongFileAuthorizationError",
                "description": (
                    "Endpoint file ID did not match file ID announced in work"
                    " order token."
                ),
                "data": {},
            },
        )
    return None


def _no_such_object(file_id: str) -> httpx2.Response:
    """Report the DRS object as unknown, the way the Download API does."""
    return httpx2.Response(
        404,
        json={
            "exception_id": "noSuchObject",
            "description": f'The DRSObject with the id "{file_id}" does not exist.',
            "data": {"file_id": file_id},
        },
    )


def serve_download_api(
    download_api: DownloadApiMock,
    s3_fixture: S3Fixture,  # noqa: F811
    staged: StagedObject | None = None,
    *,
    expires_after: int = SHORT_LIFESPAN,
) -> None:
    """Answer Download API requests the way the real Download API would.

    `staged` is the one object that is ready for download. Its S3 URL is presigned anew
    on every request, so it can carry the short lifespan these tests need without the
    object ever becoming unreachable - which is the point, since expiring URLs are what
    makes the connector refresh them. Any other file ID is reported as still being
    staged or as unknown, and a work order token that doesn't check out is refused.
    """

    async def get_drs_object(request: httpx2.Request, file_id: str) -> httpx2.Response:
        """Describe the object, or explain why it cannot be downloaded."""
        if refusal := _refused_work_order_token(request):
            return refusal
        if file_id == RETRY_FILE_ID:
            return httpx2.Response(
                202, headers={"Retry-After": "10", "Cache-Control": "no-store"}
            )
        if staged is None or file_id != staged.file_id:
            return _no_such_object(file_id)

        download_url = await s3_fixture.storage.get_object_download_url(
            bucket_id=staged.bucket_id,
            object_id=staged.file_id,
            expires_after=expires_after,
        )
        now = now_as_utc().isoformat()
        return httpx2.Response(
            200,
            json={
                "file_id": staged.file_id,
                "self_uri": f"drs://localhost:8080//{staged.file_id}",
                "size": staged.size,
                "created_time": now,
                "updated_time": now,
                "checksums": [{"checksum": "1", "type": "md5"}],
                "access_methods": [{"access_url": {"url": download_url}, "type": "s3"}],
            },
            headers=caching_headers(expires_after),
        )

    def get_envelope(request: httpx2.Request, file_id: str) -> httpx2.Response:
        """Hand out the Crypt4GH envelope, for the objects that have one."""
        if staged is None or file_id != staged.file_id or staged.envelope is None:
            return _no_such_object(file_id)
        return envelope_response(staged.envelope)

    download_api.on_get_drs_object = get_drs_object
    download_api.on_get_envelope = get_envelope


@pytest.mark.parametrize(
    "file_size, part_size",
    [
        # first test with some very small files size
        (8, 1024),
        (32, 1024),
        (128, 1024),
        (512, 1024),
        (1024, 1024),
        (2048, 1024),
        (20 * 1024, 1024),
        # then test with larger files sizes
        (6 * 1024 * 1024, 5 * 1024 * 1024),
        (12 * 1024 * 1024, 5 * 1024 * 1024),
        (20 * 1024 * 1024, 1 * 1024 * 1024),
        (1 * 1024 * 1024, DEFAULT_PART_SIZE),
        (75 * 1024 * 1024, 10 * 1024 * 1024),
    ],
)
async def test_multipart_download(
    file_size: int,
    part_size: int,
    s3_fixture: S3Fixture,  # noqa F811
    tmp_path: pathlib.Path,
    monkeypatch,
    mock_apis: MockApis,  # noqa: F811
    patch_work_package_functions,  # noqa: F811
):
    """Test the multipart download of a file"""
    # override the default config fixture with updated part size
    monkeypatch.setattr(
        "ghga_connector.config.CONFIG", get_test_config(part_size=part_size)
    )

    big_object = await get_big_s3_object(s3_fixture, object_size=file_size)

    mock_health_checks(monkeypatch)

    # Patch get_package_files
    monkeypatch.setattr(
        GET_PACKAGE_FILES_ATTR,
        AsyncMock(return_value={big_object.object_id: ""}),
    )

    # right now the desired file size is only
    # approximately met by the provided big file:
    actual_file_size = len(big_object.content)

    serve_download_api(
        mock_apis.download,
        s3_fixture,
        StagedObject(
            file_id=big_object.object_id,
            bucket_id=big_object.bucket_id,
            size=actual_file_size,
        ),
    )

    big_file_content = FAKE_ENVELOPE + big_object.content

    await async_download(
        output_dir=tmp_path,
        my_public_key_path=Path(PUBLIC_KEY_FILE),
        my_private_key_path=Path(PRIVATE_KEY_FILE),
    )

    with open(tmp_path / f"{big_object.object_id}{C4GH}", "rb") as file:
        observed_content = file.read()

    assert len(observed_content) == len(big_file_content)
    assert observed_content == big_file_content


@pytest.mark.parametrize(
    "bad_outdir,file_name,expected_exception",
    [
        (False, "file_downloadable", nullcontext()),
        (False, "file_retry", pytest.raises(exceptions.MaxWaitTimeExceededError)),
        (
            True,
            "file_downloadable",
            pytest.raises(exceptions.DirectoryDoesNotExistError),
        ),
        (False, "file_envelope_missing", pytest.raises(exceptions.GetEnvelopeError)),
    ],
)
async def test_download(
    bad_outdir: bool,
    file_name: str,
    expected_exception: Any,
    s3_fixture: S3Fixture,  # noqa: F811
    tmp_path: pathlib.Path,
    monkeypatch,
    mock_apis: MockApis,  # noqa: F811
    patch_work_package_functions,  # noqa: F811
):
    """Test the download of a file"""
    output_dir = Path("/non/existing/path") if bad_outdir else tmp_path

    # Patch get_package_files
    file = state.FILES[file_name]
    monkeypatch.setattr(
        GET_PACKAGE_FILES_ATTR,
        AsyncMock(return_value={file.file_id: ""}),
    )

    # The envelope is only served for files that have one - "envelope-missing" doesn't.
    staged = (
        StagedObject(
            file_id=file.file_id,
            bucket_id=file.grouping_label,
            size=os.path.getsize(file.file_path),
            envelope=None if file_name == "file_envelope_missing" else FAKE_ENVELOPE,
        )
        if file.populate_storage
        else None
    )
    serve_download_api(mock_apis.download, s3_fixture, staged)

    mock_health_checks(monkeypatch)

    with expected_exception:
        await async_download(
            output_dir=output_dir,
            my_public_key_path=Path(PUBLIC_KEY_FILE),
            my_private_key_path=Path(PRIVATE_KEY_FILE),
        )

    tmp_file = tmp_path / "file_with_envelope"

    # Copy fake envelope into new temp file, then append the test file
    with tmp_file.open("wb") as file_write:
        with file.file_path.open("rb") as file_read:
            buffer = file_read.read()
            file_write.write(FAKE_ENVELOPE)
            file_write.write(buffer)

    if not expected_exception:
        assert cmp(output_dir / f"{file.file_id}{C4GH}", tmp_file)


async def test_file_not_downloadable(
    s3_fixture: S3Fixture,  # noqa: F811
    tmp_path: pathlib.Path,
    monkeypatch,
    mock_apis: MockApis,  # noqa: F811
    patch_work_package_functions,  # noqa: F811
):
    """Test to try downloading a file that isn't in storage.

    Tests for 403 error behavior as well as the case where an expected file ID is
    reported missing by the download controller API (and the user chooses not to
    continue the download).
    """
    output_dir = tmp_path

    mock_health_checks(monkeypatch)

    # Patch get_package_files
    file = state.FILES["file_not_downloadable"]
    monkeypatch.setattr(
        GET_PACKAGE_FILES_ATTR,
        AsyncMock(return_value={file.file_id: ""}),
    )

    # Nothing is staged, so the Download API reports the file as unknown
    serve_download_api(mock_apis.download, s3_fixture)

    # 403 caused by an invalid auth token
    with (
        patch(
            "ghga_connector.core.work_package._decrypt",
            lambda data, key: AUTH_FAILURE_TOKEN,
        ),
        pytest.raises(
            exceptions.UnauthorizedAPICallError,
            match=r"This is not the token you're looking for\.",
        ),
    ):
        await async_download(
            output_dir=output_dir,
            my_public_key_path=Path(PUBLIC_KEY_FILE),
            my_private_key_path=Path(PRIVATE_KEY_FILE),
        )

    # 403 caused by requesting file ID that's not part of the work order token
    with (
        patch(
            "ghga_connector.core.work_package._decrypt",
            lambda data, key: FILE_ID_MISMATCH_TOKEN,
        ),
        pytest.raises(
            exceptions.UnauthorizedAPICallError,
            match="Endpoint file ID did not match file ID"
            " announced in work order token",
        ),
    ):
        await async_download(
            output_dir=output_dir,
            my_public_key_path=Path(PUBLIC_KEY_FILE),
            my_private_key_path=Path(PRIVATE_KEY_FILE),
        )

    # Exception arising when the file ID is valid, but not found in the Download API (and the
    #  user inputs 'no' instead of 'yes' when prompted if they want to continue anyway)
    with (
        patch(
            "ghga_connector.core.downloading.batch_processing.CliIoHandler.get_input",
            return_value="no",
        ),
        pytest.raises(exceptions.AbortBatchProcessError),
    ):
        await async_download(
            output_dir=output_dir,
            my_public_key_path=Path(PUBLIC_KEY_FILE),
            my_private_key_path=Path(PRIVATE_KEY_FILE),
        )


async def test_download_bad_url(
    tmp_path: pathlib.Path,
    monkeypatch,
    mock_apis: MockApis,  # noqa: F811
    patch_work_package_functions,  # noqa: F811
):
    """Check that the right error is raised for a bad URL in the download logic."""
    mock_health_checks(monkeypatch, reachable=False)

    # Patch get_package_files
    file = state.FILES["file_downloadable"]
    monkeypatch.setattr(
        GET_PACKAGE_FILES_ATTR,
        AsyncMock(return_value={file.file_id: ""}),
    )

    with pytest.raises(exceptions.ApiNotReachableError):
        await async_download(
            output_dir=tmp_path,
            my_public_key_path=Path(PUBLIC_KEY_FILE),
            my_private_key_path=Path(PRIVATE_KEY_FILE),
        )
