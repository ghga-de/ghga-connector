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

from typing import Union

import httpx

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls.work_package import WorkPackageAccessor
from ghga_connector.core.constants import TIMEOUT_LONG
from ghga_connector.core.downloading.request_dataclasses import (
    RetryResponse,
    UrlAndHeaders,
    URLResponse,
)


def _get_authorization(
    file_id: str, work_package_accessor: WorkPackageAccessor, url: str
) -> UrlAndHeaders:
    """
    Fetch work order token using accessor and prepare DCS endpoint URL and headers for a
    given endpoint identified by the `url` passed
    """
    # fetch a work order token
    decrypted_token = work_package_accessor.get_work_order_token(file_id=file_id)
    # build headers
    headers = httpx.Headers(
        {
            "Accept": "application/json",
            "Authorization": f"Bearer {decrypted_token}",
            "Content-Type": "application/json",
        }
    )

    return UrlAndHeaders(endpoint_url=url, headers=headers)


def get_envelope_authorization(
    file_id: str, work_package_accessor: WorkPackageAccessor
) -> UrlAndHeaders:
    """
    Fetch work order token using accessor and prepare DCS endpoint URL and headers to get
    a Crypt4GH envelope for file identified by `file_id`
    """
    # build url
    url = f"{work_package_accessor.dcs_api_url}/objects/{file_id}/envelopes"
    return _get_authorization(
        file_id=file_id, work_package_accessor=work_package_accessor, url=url
    )


def get_file_authorization(
    file_id: str, work_package_accessor: WorkPackageAccessor
) -> UrlAndHeaders:
    """
    Fetch work order token using accessor and prepare DCS endpoint URL and headers to get
    object storage URL for file download
    """
    # build URL
    url = f"{work_package_accessor.dcs_api_url}/objects/{file_id}"
    return _get_authorization(
        file_id=file_id, work_package_accessor=work_package_accessor, url=url
    )


def get_download_url(
    *,
    client: httpx.Client,
    url_and_headers: UrlAndHeaders,
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
    url = url_and_headers.endpoint_url

    try:
        response = client.get(
            url=url, headers=url_and_headers.headers, timeout=TIMEOUT_LONG
        )
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

    return URLResponse(
        download_url=download_url,
        file_size=file_size,
    )
