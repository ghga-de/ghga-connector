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

from os import path

import pytest
import typer

from ghga_connector.cli import (
    ApiNotReachable,
    DirectoryNotExist,
    MaxWaitTimeExceeded,
    download,
    upload,
)
from ghga_connector.core import (
    BadResponseCodeError,
    RequestFailedError,
    confirm_api_call,
)

from ..fixtures import s3_fixture  # noqa: F401
from ..fixtures import state
from ..fixtures.mock_api.testcontainer import MockAPIContainer
from ..fixtures.utils import BASE_DIR

EXAMPLE_FOLDER = path.join(BASE_DIR.parent.parent.resolve(), "example_data")


@pytest.mark.parametrize(
    "bad_url,file_id,output_dir,max_wait_time,expected_exception",
    [
        (True, "downloadable", EXAMPLE_FOLDER, "60", ApiNotReachable),
        (False, "downloadable", EXAMPLE_FOLDER, "60", None),
        (False, "not_downloadable", EXAMPLE_FOLDER, "60", BadResponseCodeError),
        (False, "retry", EXAMPLE_FOLDER, "60", MaxWaitTimeExceeded),
        (False, "downloadable", "/this_path/", "60", DirectoryNotExist),
    ],
)
def test_download(
    bad_url,
    file_id,
    output_dir,
    max_wait_time,
    expected_exception,
    s3_fixture,  # noqa F811
):
    """Test the download of a file, expects Abort, if the file was not found"""

    downloadable_file = state.FILES["file_downloadable"]
    download_url = s3_fixture.storage.get_object_download_url(
        bucket_id=downloadable_file.grouping_label,
        object_id=downloadable_file.file_id,
        expires_after=60,
    )

    with MockAPIContainer(s3_download_url=download_url) as api:
        api_url = "http://bad_url" if bad_url else api.get_connection_url()

        try:
            download(
                api_url=api_url,
                file_id=file_id,
                output_dir=output_dir,
                max_wait_time=int(max_wait_time),
            )
            assert expected_exception is None
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

    uploadeable_file = state.FILES[file_name]
    upload_url = s3_fixture.storage.get_object_upload_url(
        bucket_id=uploadeable_file.grouping_label,
        object_id=uploadeable_file.file_id,
        expires_after=60,
    )

    with MockAPIContainer(
        s3_upload_url=upload_url.url, s3_upload_fields=upload_url.fields
    ) as api:
        api_url = "http://bad_url" if bad_url else api.get_connection_url()

        try:
            upload(
                api_url,
                uploadeable_file.file_id,
                str(uploadeable_file.file_path.resolve()),
            )
            assert expected_exception is None
        except Exception as exception:
            assert isinstance(exception, expected_exception)


@pytest.mark.parametrize(
    "bad_url,file_id,expected_exception",
    [
        (False, "uploaded", None),
        (False, "not_uploaded", BadResponseCodeError),
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
