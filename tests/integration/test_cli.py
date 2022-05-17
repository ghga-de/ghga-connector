# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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
from filecmp import cmp
from pathlib import Path

import pytest
import typer
from ghga_service_chassis_lib.object_storage_dao_testing import (
    ObjectFixture,
    upload_file,
)
from ghga_service_chassis_lib.utils import big_temp_file

from ghga_connector.cli import (
    DEFAULT_PART_SIZE,
    ApiNotReachable,
    DirectoryNotExist,
    download,
    upload,
)
from ghga_connector.core import (
    BadResponseCodeError,
    MaxWaitTimeExceeded,
    RequestFailedError,
    confirm_api_call,
)

from ..fixtures import s3_fixture  # noqa: F401
from ..fixtures import state
from ..fixtures.mock_api.testcontainer import MockAPIContainer


@pytest.mark.parametrize(
    "file_size,part_size",
    [
        (6 * 1024 * 1024, 5 * 1024 * 1024),
        (12 * 1024 * 1024, 5 * 1024 * 1024),
        (6 * 1024 * 1024, DEFAULT_PART_SIZE),
        (20 * 1024 * 1024, DEFAULT_PART_SIZE),
    ],
)
def test_multipart_download(
    file_size,
    part_size,
    s3_fixture,  # noqa F811
    tmp_path,
):
    """Test the multipart download of a file"""

    with big_temp_file(file_size) as big_file:

        object_fixture = ObjectFixture(
            file_path=big_file.name, bucket_id="outbox", object_id="big-downloadable"
        )

        # upload file to s3
        if not s3_fixture.storage.does_bucket_exist(object_fixture.bucket_id):
            s3_fixture.storage.create_bucket(object_fixture.bucket_id)

        if not s3_fixture.storage.does_object_exist(
            bucket_id=object_fixture.bucket_id, object_id=object_fixture.object_id
        ):
            presigned_post = s3_fixture.storage.get_object_upload_url(
                bucket_id=object_fixture.bucket_id, object_id=object_fixture.object_id
            )
            upload_file(
                presigned_url=presigned_post,
                file_path=big_file.name,
                file_md5=object_fixture.md5,
            )

        # get s3 download url
        download_url = s3_fixture.storage.get_object_download_url(
            bucket_id=object_fixture.bucket_id,
            object_id=object_fixture.object_id,
            expires_after=180,
        )
        with MockAPIContainer(
            s3_download_url=download_url,
            s3_download_file_size=os.path.getsize(object_fixture.file_path),
        ) as api:
            api_url = api.get_connection_url()

            try:
                download(
                    api_url=api_url,
                    file_id=object_fixture.object_id,
                    output_dir=tmp_path,
                    max_wait_time=int(60),
                    part_size=part_size,
                    max_retries=0,
                )
                assert cmp(
                    tmp_path / object_fixture.object_id, object_fixture.file_path
                )
            except Exception as exception:
                raise exception


@pytest.mark.parametrize(
    "file_size,part_size",
    [
        (6 * 1024 * 1024, 5 * 1024 * 1024),
        (12 * 1024 * 1024, 5 * 1024 * 1024),
        (6 * 1024 * 1024, DEFAULT_PART_SIZE),
        (20 * 1024 * 1024, DEFAULT_PART_SIZE),
    ],
)
def test_multipart_download(
    file_size,
    part_size,
    s3_fixture,  # noqa F811
    tmp_path,
):
    """Test the multipart download of a file"""

    with big_temp_file(file_size) as big_file:

        object_fixture = ObjectFixture(
            file_path=big_file.name,
            bucket_id=s3_fixture.existing_buckets[0],
            object_id="big-downloadable",
        )

        # upload file to s3
        assert not s3_fixture.storage.does_object_exist(
            bucket_id=object_fixture.bucket_id, object_id=object_fixture.object_id
        )
        presigned_post = s3_fixture.storage.get_object_upload_url(
            bucket_id=object_fixture.bucket_id, object_id=object_fixture.object_id
        )
        upload_file(
            presigned_url=presigned_post,
            file_path=big_file.name,
            file_md5=object_fixture.md5,
        )

        # get s3 download url
        download_url = s3_fixture.storage.get_object_download_url(
            bucket_id=object_fixture.bucket_id,
            object_id=object_fixture.object_id,
            expires_after=180,
        )
        with MockAPIContainer(
            s3_download_url=download_url,
            s3_download_file_size=os.path.getsize(object_fixture.file_path),
        ) as api:
            api_url = api.get_connection_url()

            try:
                download(
                    api_url=api_url,
                    file_id=object_fixture.object_id,
                    output_dir=tmp_path,
                    max_wait_time=int(60),
                    part_size=part_size,
                    max_retries=0,
                )
                assert cmp(
                    tmp_path / object_fixture.object_id, object_fixture.file_path
                )
            except Exception as exception:
                raise exception


@pytest.mark.parametrize(
    "bad_url,bad_outdir,file_name,max_wait_time,expected_exception",
    [
        (True, False, "file_downloadable", "60", ApiNotReachable),
        (False, False, "file_downloadable", "60", None),
        (
            False,
            False,
            "file_not_downloadable",
            "60",
            BadResponseCodeError,
        ),
        (False, False, "file_retry", "60", MaxWaitTimeExceeded),
        (False, True, "file_downloadable", "60", DirectoryNotExist),
    ],
)
def test_download(
    bad_url,
    bad_outdir,
    file_name,
    max_wait_time,
    expected_exception,
    s3_fixture,  # noqa F811
    tmp_path,
):
    """Test the download of a file"""

    output_dir = Path("/non/existing/path") if bad_outdir else tmp_path

    file = state.FILES[file_name]

    if file.populate_storage:

        download_url = s3_fixture.storage.get_object_download_url(
            bucket_id=file.grouping_label,
            object_id=file.file_id,
            expires_after=60,
        )

    else:
        download_url = ""

    with MockAPIContainer(
        s3_download_url=download_url,
        s3_download_file_size=os.path.getsize(file.file_path),
    ) as api:
        api_url = "http://bad_url" if bad_url else api.get_connection_url()

        try:
            download(
                api_url=api_url,
                file_id=file.file_id,
                output_dir=output_dir,
                max_wait_time=int(max_wait_time),
                part_size=DEFAULT_PART_SIZE,
                max_retries=0,
            )
            assert expected_exception is None
            assert cmp(output_dir / file.file_id, file.file_path)
        except Exception as exception:
            assert isinstance(exception, expected_exception)


@pytest.mark.parametrize(
    "bad_url,file_name,expected_exception",
    [
        (True, "file_uploadable", typer.Abort),
        (False, "file_uploadable", None),
        (False, "file_not_uploadable", typer.Abort),
        (False, "file_with_bad_path", typer.Abort),
    ],
)
def test_upload(
    bad_url,
    file_name,
    expected_exception,
    s3_fixture,  # noqa F811
):
    """Test the upload of a file, expects Abort, if the file was not found"""

    uploadable_file = state.FILES[file_name]
    upload_url = s3_fixture.storage.get_object_upload_url(
        bucket_id=uploadable_file.grouping_label,
        object_id=uploadable_file.file_id,
        expires_after=60,
    )

    with MockAPIContainer(
        s3_upload_url=upload_url.url, s3_upload_fields=upload_url.fields
    ) as api:
        api_url = "http://bad_url" if bad_url else api.get_connection_url()

        try:
            upload(
                api_url,
                uploadable_file.file_id,
                str(uploadable_file.file_path.resolve()),
            )
            assert expected_exception is None
            assert s3_fixture.storage.does_object_exist(
                bucket_id=uploadable_file.grouping_label,
                object_id=uploadable_file.file_id,
            )
        except Exception as exception:
            assert isinstance(exception, expected_exception)


@pytest.mark.parametrize(
    "bad_url,file_id,expected_exception",
    [
        (False, "uploaded", None),
        (False, "uploadable", BadResponseCodeError),
        (True, "uploaded", RequestFailedError),
    ],
)
def test_confirm_api_call(
    bad_url,
    file_id,
    expected_exception,
):
    """
    Test the confirm_api_call function
    """
    with MockAPIContainer() as api:
        api_url = "http://bad_url" if bad_url else api.get_connection_url()

        try:
            confirm_api_call(api_url=api_url, file_id=file_id)
            assert expected_exception is None
        except Exception as exception:
            assert isinstance(exception, expected_exception)
