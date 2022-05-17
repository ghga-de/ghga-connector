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

"""Tests for the core functions of the cli"""

from filecmp import cmp
from os import path

import pytest

from ghga_connector.core import check_url, download_file_part

from ..fixtures import s3_fixture  # noqa: F401
from ..fixtures import state


@pytest.mark.parametrize(
    "api_url,wait_time,expected_response",
    # Google has a higher availability than ghga.de
    [("https://www.google.de/", 1000, True), ("https://bad_url", 1000, False)],
)
def test_check_url(api_url, wait_time, expected_response):
    """
    Test the check_url function
    """
    response = check_url(api_url, wait_time)
    assert response == expected_response


def test_download_file(
    s3_fixture,  # noqa F811
    tmp_path,
):
    """
    Test the download_file function
    """

    file_path = tmp_path / "file.test"

    downloadable_file = state.FILES["file_downloadable"]
    download_url = s3_fixture.storage.get_object_download_url(
        bucket_id=downloadable_file.grouping_label,
        object_id=downloadable_file.file_id,
        expires_after=3600,
    )

    # Try to download the whole test file in one part. Should be fairly small.
    download_file_part(
        download_url=download_url,
        output_file_path=file_path,
        part_offset=0,
        part_size=path.getsize(downloadable_file.file_path),
        file_size=path.getsize(downloadable_file.file_path),
    )

    assert cmp(file_path, downloadable_file.file_path)
