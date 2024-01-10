# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""TODO"""

from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Union

from ghga_connector.core import exceptions
from ghga_connector.core.dataclasses import PartRange
from ghga_connector.core.file_operations import calc_part_ranges
from ghga_connector.core.message_display import AbstractMessageDisplay


@dataclass
class RetryResponse:
    """TODO"""

    retry_after: int


@dataclass
class URLResponse:
    """TODO"""

    download_url: str
    file_size: int


class DownloaderBase(ABC):
    """TODO"""

    @abstractmethod
    def await_download_url(
        self,
        *,
        max_wait_time: int,
        message_display: AbstractMessageDisplay,
    ) -> URLResponse:
        """Wait until download URL can be generated.
        Returns a URLResponse containing two elements:
            1. the download url
            2. the file size in bytes
        """

    @abstractmethod
    def get_download_url(self) -> Union[RetryResponse, URLResponse]:
        """
        Perform a RESTful API call to retrieve a presigned download URL.
        Returns:
            If the download url is not available yet, a RetryResponse is returned,
            containing the time in seconds after which the download url should become
            available.
            Otherwise, a URLResponse containing the download url and file size in bytes
            is returned.
        """

    @abstractmethod
    def get_download_urls(
        self,
    ) -> Iterator[URLResponse]:
        """
        For the multi-part upload identified by the `file_id`, it returns an
        iterator to obtain download_urls.
        """

    @abstractmethod
    def get_file_header_envelope(self) -> bytes:
        """
        Perform a RESTful API call to retrieve a file header envelope.
        Returns:
            The file header envelope (bytes object)
        """

    @abstractmethod
    def download_content_range(
        self,
        *,
        download_url: str,
        start: int,
        end: int,
        queue: Queue,
    ) -> None:
        """Download a specific range of a file's content using a presigned download url."""

    @abstractmethod
    def download_file_parts(
        self,
        *,
        url_response: Iterator[URLResponse],
        max_concurrent_downloads: int,
        part_ranges: Sequence[PartRange],
        queue: Queue,
    ) -> None:
        """Download stuff"""


def run_download(
    downloader: DownloaderBase,
    max_wait_time: int,
    message_display: AbstractMessageDisplay,
    output_file_ongoing: Path,
    part_size: int,
):
    """TODO"""
    # stage download and get file size
    url_response = downloader.await_download_url(
        max_wait_time=max_wait_time,
        message_display=message_display,
    )

    # get file header envelope
    try:
        envelope = downloader.get_file_header_envelope()
    except (
        exceptions.FileNotRegisteredError,
        exceptions.EnvelopeNotFoundError,
        exceptions.ExternalApiError,
    ) as error:
        raise exceptions.GetEnvelopeError() from error

    # perform the download
    try:
        download_parts(
            downloader=downloader,
            envelope=envelope,
            output_file=output_file_ongoing,
            part_size=part_size,
            file_size=url_response.file_size,
        )
    except (
        exceptions.ConnectionFailedError,
        exceptions.NoS3AccessMethodError,
    ) as error:
        # Remove file if the download failed.
        output_file_ongoing.unlink()
        raise exceptions.DownloadError() from error


def download_parts(  # noqa: PLR0913
    *,
    downloader: DownloaderBase,
    max_concurrent_downloads: int = 5,
    max_queue_size: int = 10,
    part_size: int,
    file_size: int,
    output_file: Path,
    envelope: bytes,
):
    """
    Downloads a file from the given URL using multiple threads and saves it to a file.

    :param max_concurrent_downloads: Maximum number of parallel downloads.
    :param max_queue_size: Maximum size of the queue.
    :param part_size: Size of each part to download.
    """
    # Create a queue object to store downloaded parts
    queue: Queue = Queue(maxsize=max_queue_size)

    # Split the file into parts based on the part size
    part_ranges = calc_part_ranges(part_size=part_size, total_file_size=file_size)

    # Get the download urls
    download_urls = downloader.get_download_urls()

    # Download the file parts in parallel
    downloader.download_file_parts(
        max_concurrent_downloads=max_concurrent_downloads,
        queue=queue,
        part_ranges=part_ranges,
        url_response=download_urls,
    )

    # Write the downloaded parts to a file
    with output_file.open("wb") as file:
        # put envelope in file
        file.write(envelope)
        offset = len(envelope)
        downloaded_size = 0
        while downloaded_size < file_size:
            try:
                start, part = queue.get(block=False)
            except Empty:
                continue
            file.seek(offset + start)
            file.write(part)
            downloaded_size += len(part)
            queue.task_done()
