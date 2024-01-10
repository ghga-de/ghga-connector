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

"""This file contains all api calls related to uploading files"""

import base64
import concurrent.futures
from collections.abc import Iterator, Sequence
from queue import Queue
from time import sleep
from typing import Any, Union

import httpx

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls.work_package import WorkPackageAccessor
from ghga_connector.core.constants import TIMEOUT
from ghga_connector.core.dataclasses import PartRange
from ghga_connector.core.download.download_handler import (
    DownloaderBase,
    RetryResponse,
    URLResponse,
)
from ghga_connector.core.http_translation import ResponseExceptionTranslator
from ghga_connector.core.message_display import AbstractMessageDisplay


class Downloader(DownloaderBase):
    """TODO"""

    def __init__(
        self,
        *,
        file_id: str,
        work_package_accessor: WorkPackageAccessor,
        client: httpx.Client,
    ):
        self._client = client
        self._file_id = file_id
        self._work_package_accessor = work_package_accessor

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
        # get the download_url, wait if needed
        wait_time = 0
        while wait_time < max_wait_time:
            try:
                response = self.get_download_url()
            except exceptions.BadResponseCodeError as error:
                message_display.failure(
                    "The request was invalid and returned a bad HTTP status code."
                )
                raise error
            except exceptions.RequestFailedError as error:
                message_display.failure("The request failed.")
                raise error

            if isinstance(response, RetryResponse):
                # typechecker is a bit weird here. Could skip the else and indentation,
                # but but then it thinks response for this branch is of type URLResponse
                retry_time = response.retry_after
                wait_time += retry_time
                message_display.display(
                    f"File staging, will try to download again in {retry_time} seconds"
                )
                sleep(retry_time)
            else:
                return response

        raise exceptions.MaxWaitTimeExceededError(max_wait_time=max_wait_time)

    def download_content_range(
        self,
        *,
        download_url: str,
        start: int,
        end: int,
        queue: Queue,
    ) -> None:
        """Download a specific range of a file's content using a presigned download url."""
        headers = {"Range": f"bytes={start}-{end}"}
        try:
            response = self._client.get(download_url, headers=headers, timeout=TIMEOUT)
        except httpx.RequestError as request_error:
            exceptions.raise_if_connection_failed(
                request_error=request_error, url=download_url
            )
            raise exceptions.RequestFailedError(url=download_url) from request_error

        status_code = response.status_code

        # 200, if the full file was returned, 206 else.
        if status_code in (200, 206):
            queue.put((start, response.content))
            return

        raise exceptions.BadResponseCodeError(
            url=download_url, response_code=status_code
        )

    def download_file_parts(
        self,
        *,
        url_response: Iterator[URLResponse],
        max_concurrent_downloads: int,
        part_ranges: Sequence[PartRange],
        queue: Queue[tuple[int, bytes]],
    ) -> None:
        """Download stuff"""
        # Download the parts using a thread pool executor
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_downloads,
        )

        for part_range, download_url in zip(part_ranges, url_response):
            kwargs: dict[str, Any] = {
                "download_url": download_url,
                "start": part_range.start,
                "end": part_range.stop,
                "queue": queue,
            }

            executor.submit(self.download_content_range, **kwargs)

    def get_download_url(
        self,
    ) -> Union[RetryResponse, URLResponse]:
        """
        Perform a RESTful API call to retrieve a presigned download URL.
        Returns:
            If the download url is not available yet, a RetryResponse is returned,
            containing the time in seconds after which the download url should become
            available.
            Otherwise, a URLResponse containing the download url and file size in bytes
            is returned.
        """
        return get_download_url(
            client=self._client,
            file_id=self._file_id,
            work_package_accessor=self._work_package_accessor,
        )

    def get_download_urls(
        self,
    ) -> Iterator[URLResponse]:
        """
        For a specific multi-part upload identified by the `file_id`, it returns an
        iterator to obtain download_urls.
        """
        while True:
            url_response = self.get_download_url()
            if isinstance(url_response, RetryResponse):
                raise exceptions.UnexcpectedRetryResponseError()
            yield url_response

    def get_file_header_envelope(self) -> bytes:
        """
        Perform a RESTful API call to retrieve a file header envelope.
        Returns:
            The file header envelope (bytes object)
        """
        # fetch a work order token
        decrypted_token = self._work_package_accessor.get_work_order_token(
            file_id=self._file_id
        )

        # build url and headers
        url = f"{self._work_package_accessor.dcs_api_url}/objects/{self._file_id}/envelopes"

        headers = httpx.Headers(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {decrypted_token}",
                "Content-Type": "application/json",
            }
        )

        # Make function call to get download url
        try:
            response = self._client.get(url=url, headers=headers, timeout=TIMEOUT)
        except httpx.RequestError as request_error:
            raise exceptions.RequestFailedError(url=url) from request_error

        status_code = response.status_code

        if status_code == 200:
            return base64.b64decode(response.content)

        # For now unauthorized responses are not handled by httpyexpect
        if status_code == 403:
            content = response.json()
            # handle both normal and httpyexpect 403 response
            if "description" in content:
                cause = content["description"]
            else:
                cause = content["detail"]
            raise exceptions.UnauthorizedAPICallError(url=url, cause=cause)

        spec = {
            404: {
                "envelopeNotFoundError": lambda: exceptions.EnvelopeNotFoundError(
                    file_id=self._file_id
                ),
                "noSuchObject": lambda: exceptions.FileNotRegisteredError(
                    file_id=self._file_id
                ),
            },
            500: {"externalAPIError": exceptions.ExternalApiError},
        }

        ResponseExceptionTranslator(spec=spec).handle(response=response)
        raise exceptions.BadResponseCodeError(url=url, response_code=status_code)


def get_download_url(
    client: httpx.Client, file_id: str, work_package_accessor: WorkPackageAccessor
) -> Union[RetryResponse, URLResponse]:
    """
    Perform a RESTful API call to retrieve a presigned download URL.
    Returns:
        If the download url is not available yet, a RetryResponse is returned,
        containing the time in seconds after which the download url should become
        available.
        Otherwise, a URLResponse containing the download url and file size in bytes
        is returned.
    """
    # fetch a work order token
    decrypted_token = work_package_accessor.get_work_order_token(file_id=file_id)

    # build url and headers
    url = f"{work_package_accessor.dcs_api_url}/objects/{file_id}"
    headers = httpx.Headers(
        {
            "Accept": "application/json",
            "Authorization": f"Bearer {decrypted_token}",
            "Content-Type": "application/json",
        }
    )

    # Make function call to get download url
    try:
        response = client.get(url=url, headers=headers, timeout=TIMEOUT)
    except httpx.RequestError as request_error:
        exceptions.raise_if_connection_failed(request_error=request_error, url=url)
        raise exceptions.RequestFailedError(url=url) from request_error

    status_code = response.status_code
    if status_code != 200:
        if status_code == 403:
            content = response.json()
            # handle both normal and httpyexpect 403 response
            if "description" in content:
                cause = content["description"]
            else:
                cause = content["detail"]
            raise exceptions.UnauthorizedAPICallError(url=url, cause=cause)
        if status_code != 202:
            raise exceptions.BadResponseCodeError(url=url, response_code=status_code)

        headers = response.headers
        if "retry-after" not in headers:
            raise exceptions.RetryTimeExpectedError(url=url)

        return RetryResponse(retry_after=int(headers["retry-after"]))

    # look for an access method of type s3 in the response:
    response_body = response.json()
    download_url = None
    access_methods = response_body["access_methods"]
    for access_method in access_methods:
        if access_method["type"] == "s3":
            download_url = access_method["access_url"]["url"]
            file_size = response_body["size"]
            break
    else:
        raise exceptions.NoS3AccessMethodError(url=url)

    if download_url is None:
        raise exceptions.NoS3AccessMethodError(url=url)

    return URLResponse(
        download_url=download_url,
        file_size=file_size,
    )
