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

import pytest
import typer

from ghga_connector.cli import download, upload


@pytest.mark.parametrize(
    "api_url,file_id,output_dir,expected_exception",
    [
        ("https://www.ghga.de", 1, "/workspace/example_data/", None),
        ("https://www.ghga.de", 1, "/this_path/", typer.Abort()),
    ],
)
def test_download(api_url, file_id, output_dir, expected_exception):

    """Test the download of a file, expects Abort, if the file was not found"""

    try:
        download(api_url, file_id, output_dir)
    except Exception as exception:
        assert exception == expected_exception

    assert expected_exception is None


@pytest.mark.parametrize(
    "api_url,file_id,file_path,expected_exception",
    [
        ("https://www.ghga.de", 1, "/workspace/example_data/file1.test", None),
        ("https://www.ghga.de", 1, "/this_path/does_not_exist.test", typer.Abort()),
    ],
)
def test_upload(api_url, file_id, file_path, expected_exception):

    """Test the upload of a file, expects Abort, if the file was not found"""

    try:
        upload(api_url, file_id, file_path)
    except Exception as exception:
        assert exception == expected_exception

    assert expected_exception is None
