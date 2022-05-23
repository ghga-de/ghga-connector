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

"""Test file operations"""


import pytest
from ghga_service_chassis_lib.utils import big_temp_file

from ghga_connector.core.file_operations import download_file_part

from tests.fixtures.s3 import s3_fixture, S3Fixture, get_big_s3_object


@pytest.mark.parametrize(
    "part_start, part_end, file_size",
    [
        (0, 20 * 1024 * 1024 - 1, 20 * 1024 * 1024),  # download full file as one part
        (  # download intermediate part:
            5 * 1024 * 1024,
            10 * 1024 * 1024 - 1,
            20 * 1024 * 1024,
        ),
    ],
)
def test_download_file_part(
    part_start: int, part_end: int, file_size: int, s3_fixture: S3Fixture
):
    """Test the `download_file_part` function."""
    # prepare state and the expected result:
    big_object = get_big_s3_object(s3_fixture, object_size=file_size)
    download_url = s3_fixture.storage.get_object_download_url(
        object_id=big_object.object_id, bucket_id=big_object.bucket_id
    )
    expected_bytes = big_object.content[part_start : part_end + 1]

    # donwload file part wiht dedicated function:
    obtained_bytes = download_file_part(
        download_url=download_url, part_start=part_start, part_end=part_end
    )

    assert expected_bytes == obtained_bytes
