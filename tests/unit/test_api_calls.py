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

"""Tests for API Calls"""

from contextlib import nullcontext
from typing import Optional
from unittest.mock import Mock

import pytest

from ghga_connector.core import (
    CantChangeUploadStatus,
    UploadNotRegisteredError,
    UploadStatus,
    patch_multipart_upload,
)
from ghga_connector.core.api_calls import get_part_upload_urls
from ghga_connector.core.exceptions import MaxPartNoExceededError, MaxRetriesReached
from ghga_connector.core.retry import WithRetry

from ..fixtures.mock_api.testcontainer import MockAPIContainer
from ..fixtures.retry import max_retries  # noqa: F401


@pytest.mark.parametrize(
    "bad_url,upload_id,upload_status,expected_exception",
    [
        (False, "pending", UploadStatus.UPLOADED, None),
        (False, "uploaded", UploadStatus.CANCELLED, None),
        (False, "pending", UploadStatus.CANCELLED, CantChangeUploadStatus),
        (False, "uploadable", UploadStatus.UPLOADED, CantChangeUploadStatus),
        (False, "not_uploadable", UploadStatus.UPLOADED, UploadNotRegisteredError),
        (True, "uploaded", UploadStatus.UPLOADED, MaxRetriesReached),
    ],
)
def test_patch_multipart_upload(
    bad_url: bool,
    upload_id: str,
    upload_status: UploadStatus,
    expected_exception: type[Exception],
    max_retries: int,  # noqa: F811
):
    """
    Test the patch_multipart_upload function
    """
    WithRetry.set_retries(max_retries)
    with MockAPIContainer() as api:
        api_url = "http://bad_url" if bad_url else api.get_connection_url()

        try:
            patch_multipart_upload(
                api_url=api_url,
                upload_id=upload_id,
                upload_status=upload_status,
            )
            assert expected_exception is None
        except Exception as exception:
            assert isinstance(exception, expected_exception)


@pytest.mark.parametrize(
    "from_part, end_part, exception",
    [
        (None, 10, None),
        (2, 10, None),
        (9999, 10001, MaxPartNoExceededError),
    ],
)
def test_get_part_upload_urls(
    from_part: Optional[int],
    end_part: int,
    exception: Optional[Exception],
):
    """
    Test the `get_part_upload_urls` generator for iterating through signed part urls
    """
    upload_id = "example-upload"
    api_url = "http://my-api.example"
    from_part_ = 1 if from_part is None else from_part

    # mock the function to get a specific part upload url:
    static_signed_url = "http://my-signed-url.example/97982jsdf7823j"
    get_url_func = Mock(return_value=static_signed_url)

    # create the iterator:
    kwargs = {
        "api_url": api_url,
        "upload_id": upload_id,
        "get_url_func": get_url_func,
    }
    if from_part is not None:
        kwargs["from_part"] = from_part
    part_upload_urls = get_part_upload_urls(**kwargs)  # type: ignore

    with (pytest.raises(exception) if exception else nullcontext()):
        for idx, signed_url in enumerate(part_upload_urls):
            assert static_signed_url == signed_url

            part_no = idx + from_part_
            get_url_func.assert_called_with(
                api_url=api_url, upload_id=upload_id, part_no=part_no
            )

            if part_no >= end_part:
                break
