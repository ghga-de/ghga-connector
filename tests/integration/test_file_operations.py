# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from asyncio import create_task
from unittest.mock import Mock

import pytest

from ghga_connector.cli import CLIMessageDisplay
from ghga_connector.core.api_calls import WorkPackageAccessor
from ghga_connector.core.client import async_client
from ghga_connector.core.downloading import Downloader
from ghga_connector.core.downloading.downloader import TaskHandler
from ghga_connector.core.downloading.structs import URLResponse
from ghga_connector.core.file_operations import calc_part_ranges
from tests.fixtures.s3 import (  # noqa: F401
    S3Fixture,
    get_big_s3_object,
    reset_state,
    s3_fixture,
)


@pytest.mark.parametrize(
    "start, end, file_size",
    [
        # download full file as one part
        (0, 20 * 1024 * 1024 - 1, 20 * 1024 * 1024),
        (  # download intermediate part:
            5 * 1024 * 1024,
            10 * 1024 * 1024 - 1,
            20 * 1024 * 1024,
        ),
    ],
)
@pytest.mark.asyncio
async def test_download_content_range(
    start: int,
    end: int,
    file_size: int,
    s3_fixture: S3Fixture,  # noqa: F811
    monkeypatch,
):
    """Test the `download_content_range` function."""
    # prepare state and the expected result:
    big_object = await get_big_s3_object(s3_fixture, object_size=file_size)

    async def download_url(self):
        """Drop in for monkeypatching"""
        download_url = await s3_fixture.storage.get_object_download_url(
            object_id=big_object.object_id, bucket_id=big_object.bucket_id
        )
        return URLResponse(download_url=download_url, file_size=0)

    monkeypatch.setattr(
        "ghga_connector.core.downloading.downloader.Downloader.get_download_url",
        download_url,
    )
    expected_bytes = big_object.content[start : end + 1]

    message_display = CLIMessageDisplay()
    # download content range with dedicated function:
    async with async_client() as client:
        # no work package accessor calls in download_content_range, just mock for correct type
        dummy_accessor = Mock(spec=WorkPackageAccessor)
        downloader = Downloader(
            client=client,
            file_id=big_object.object_id,
            max_concurrent_downloads=5,
            max_wait_time=10,
            work_package_accessor=dummy_accessor,
            message_display=message_display,
        )
        await downloader.download_content_range(start=start, end=end)

    obtained_start, obtained_bytes = await downloader._queue.get()

    assert start == obtained_start
    assert expected_bytes == obtained_bytes


@pytest.mark.parametrize(
    "part_size",
    [5 * 1024 * 1024, 3 * 1024 * 1024, 1 * 1024 * 1024],
)
@pytest.mark.asyncio(scope="session")
async def test_download_file_parts(
    part_size: int,
    s3_fixture: S3Fixture,  # noqa: F811
    monkeypatch,
    tmp_path,
):
    """Test the `download_file_parts` function."""
    # prepare state and the expected result:
    big_object = await get_big_s3_object(s3_fixture)
    total_file_size = len(big_object.content)
    expected_bytes = big_object.content

    async def download_url(self):
        """Drop in for monkeypatching"""
        download_url = await s3_fixture.storage.get_object_download_url(
            object_id=big_object.object_id, bucket_id=big_object.bucket_id
        )
        return URLResponse(download_url=download_url, file_size=0)

    monkeypatch.setattr(
        "ghga_connector.core.downloading.downloader.Downloader.get_download_url",
        download_url,
    )
    part_ranges = calc_part_ranges(part_size=part_size, total_file_size=total_file_size)

    async with async_client() as client:
        # no work package accessor calls in download_file_parts, just mock for correct type
        dummy_accessor = Mock(spec=WorkPackageAccessor)
        message_display = CLIMessageDisplay()
        downloader = Downloader(
            client=client,
            file_id=big_object.object_id,
            max_concurrent_downloads=5,
            max_wait_time=10,
            work_package_accessor=dummy_accessor,
            message_display=message_display,
        )
        task_handler = TaskHandler()

        for part_range in part_ranges:
            await task_handler.schedule(
                downloader.download_to_queue(part_range=part_range)
            )

        file_path = tmp_path / "test.file"
        with file_path.open("wb") as file:
            dl_task = create_task(
                downloader.drain_queue_to_file(
                    file_name=file.name, file=file, file_size=total_file_size, offset=0
                )
            )
            await task_handler.finish()
            await dl_task

        num_bytes_obtained = file_path.stat().st_size
        assert num_bytes_obtained == len(expected_bytes)
