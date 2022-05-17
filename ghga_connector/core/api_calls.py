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

"""
Contains API calls to the API of the GHGA Storage implementation
"""

import json
from enum import Enum
from io import BytesIO
from time import sleep
from typing import Any, Callable, Optional, Tuple, Union

import pycurl
from ghga_service_chassis_lib.s3 import PresignedPostURL

from .exceptions import (
    BadResponseCodeError,
    MaxWaitTimeExceeded,
    NoS3AccessMethod,
    RequestFailedError,
    RetryTimeExpectedError,
)

# Constants for clarity of return values
NO_DOWNLOAD_URL = None
NO_UPLOAD_ID = None
NO_FILE_SIZE = None
NO_PART_SIZE = None
NO_RETRY_TIME = None


class UploadStatus(Enum):
    """
    Enum for the possible UploadStatus of a specific upload_id
    """

    ACCEPTED = "accepted"
    CANCELLED = "cancelled"
    FAILED = "failed"
    PENDING = "pending"
    REJECTED = "rejected"
    UPLOADED = "uploaded"


def header_function_factory(headers: dict):
    """Creates a header function that updates the specified headers dict."""

    def header_function(header_line):
        """
        Function used to decode headers from HTTP Responses
        """

        # HTTP standard specifies that headers are encoded in iso-8859-1.
        header_line = header_line.decode("iso-8859-1")

        # Header lines include the first status line (HTTP/1.x ...).
        # We are going to ignore all lines that don't have a colon in them.
        # This will botch headers that are split on multiple lines...
        if ":" not in header_line:
            return

        # Break the header line into header name and value.
        name, value = header_line.split(":", 1)

        # Remove whitespace that may be present.
        # Header lines include the trailing newline, and there may be whitespace
        # around the colon.
        name = name.strip()
        value = value.strip()

        # Header names are case insensitive.
        # Lowercase name here.
        name = name.lower()

        # Now we can actually record the header name and value.
        # Note: this only works when headers are not duplicated, see below.
        headers[name] = value

    return header_function


def initiate_multipart_upload(api_url: str, file_id: str) -> Tuple[str, int]:
    """
    Perform a RESTful API call to initiate a multipart upload on S3
    Returns an upload id and a part size
    """

    # build url
    url = api_url + "/files/" + file_id + "/uploads/"

    # Make function call to get upload url
    curl = pycurl.Curl()
    data = BytesIO()
    curl.setopt(curl.URL, url)
    curl.setopt(curl.WRITEFUNCTION, data.write)

    curl.setopt(
        curl.HTTPHEADER,
        ["Accept: application/json", "Content-Type: application/json"],
    )

    curl.setopt(curl.POST, 1)
    try:
        curl.perform()
    except pycurl.error as pycurl_error:
        raise RequestFailedError(url) from pycurl_error

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    curl.close()

    if status_code != 200:
        raise BadResponseCodeError(url, status_code)

    dictionary = json.loads(data.getvalue())

    return dictionary["upload_id"], int(dictionary["part_size"])


def part_upload(api_url: str, upload_id: str, part_no: int) -> PresignedPostURL:
    """
    Get a presigned url to upload a specific part to S3
    """

    # build url
    url = api_url + "/uploads/" + upload_id + "/parts/" + str(part_no) + "/signed_posts"

    # Make function call to get upload url
    curl = pycurl.Curl()
    data = BytesIO()
    curl.setopt(curl.URL, url)
    curl.setopt(curl.WRITEFUNCTION, data.write)

    curl.setopt(
        curl.HTTPHEADER,
        ["Accept: application/json", "Content-Type: application/json"],
    )

    curl.setopt(curl.POST, 1)
    try:
        curl.perform()
    except pycurl.error as pycurl_error:
        raise RequestFailedError(url) from pycurl_error

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    curl.close()

    if status_code != 200:
        raise BadResponseCodeError(url, status_code)

    dictionary = json.loads(data.getvalue())
    presigned_post = dictionary["presigned_post"]

    return PresignedPostURL(url=presigned_post["url"], fields=presigned_post["fields"])


def patch_multipart_upload(
    api_url: str, upload_id: str, upload_status: UploadStatus
) -> None:
    """
    Set the status of a specific upload_id.
    The API accepts "uploaded" or "accepted",
    if the upload_id is currently set to "pending"
    """
    # build url
    url = api_url + "/uploads/" + upload_id

    post_data = {"status": upload_status}
    postfields = json.dumps(post_data)

    curl = pycurl.Curl()
    curl.setopt(curl.URL, url)
    curl.setopt(
        curl.HTTPHEADER,
        ["Accept: */*", "Content-Type: application/json"],
    )
    curl.setopt(curl.POSTFIELDS, postfields)

    # Set to patch, since postfields sets to POST automatically
    curl.setopt(curl.CUSTOMREQUEST, "PATCH")

    try:
        curl.perform()
    except pycurl.error as pycurl_error:
        raise RequestFailedError(url) from pycurl_error

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    curl.close()

    if status_code != 204:
        raise BadResponseCodeError(url, status_code)


def get_pending_uploads(
    api_url: str, file_id: str
) -> Tuple[Optional[str], Optional[int]]:
    """
    Get all multipart-uploads of a specific file which are currently pending.
    This can either be 0 or 1
    Returns either the upload_id of the pending upload, or None
    """

    # build url
    url = api_url + "/files/" + file_id + "/uploads/"

    # Make function call to get upload url
    curl = pycurl.Curl()
    data = BytesIO()
    curl.setopt(curl.URL, url)
    curl.setopt(curl.WRITEFUNCTION, data.write)

    curl.setopt(
        curl.HTTPHEADER,
        ["Accept: application/json", "Content-Type: application/json"],
    )

    # GET is the standard, but setting it here explicitely nonetheless
    curl.setopt(curl.HTTPGET, 1)
    try:
        curl.perform()
    except pycurl.error as pycurl_error:
        raise RequestFailedError(url) from pycurl_error

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    curl.close()

    if status_code != 200:
        raise BadResponseCodeError(url, status_code)

    dictionary: list = json.loads(data.getvalue())

    if len(dictionary) == 0:
        return NO_UPLOAD_ID, NO_PART_SIZE

    return dictionary[0]["upload_id"], int(dictionary[0]["part_size"])


def download_api_call(
    api_url: str, file_id: str
) -> Union[Tuple[None, None, int], Tuple[str, int, None]]:
    """
    Perform a RESTful API call to retrieve a presigned download URL.
    Returns:
        A tuple of three elements:
            1. the download url
            2. the file size (in bytes)
            3. the retry-time
        If the download url is not available yet, the first two elements are None and
        the retry-time is set.
        Otherwise, only the last element is None while the others are set.
    """

    # build url
    url = api_url + "/objects/" + file_id

    # Make function call to get upload url
    curl = pycurl.Curl()
    data = BytesIO()
    headers: dict[str, str] = {}
    curl.setopt(curl.URL, url)
    curl.setopt(curl.WRITEFUNCTION, data.write)
    curl.setopt(
        curl.HTTPHEADER,
        ["Accept: application/json", "Content-Type: application/json"],
    )
    curl.setopt(curl.HEADERFUNCTION, header_function_factory(headers))
    # GET is the standard, but setting it here explicitely nonetheless
    curl.setopt(curl.HTTPGET, 1)
    try:
        curl.perform()
    except pycurl.error as pycurl_error:
        raise RequestFailedError(url) from pycurl_error

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    curl.close()

    if status_code != 200:
        if status_code != 202:
            raise BadResponseCodeError(url, status_code)

        if "retry-after" not in headers:
            raise RetryTimeExpectedError(url)

        return (NO_DOWNLOAD_URL, NO_FILE_SIZE, int(headers["retry-after"]))

    # look for an access method of type s3 in the response:
    dictionary = json.loads(data.getvalue())
    download_url = None
    access_methods = dictionary["access_methods"]
    for access_method in access_methods:
        if access_method["type"] == "s3":
            download_url = access_method["access_url"]["url"]
            file_size = dictionary["size"]
            break

    if download_url is None:
        raise NoS3AccessMethod(url)

    return download_url, file_size, NO_RETRY_TIME


def await_download_url(
    api_url: str,
    file_id: str,
    max_wait_time: int,
    logger: Callable[[str], Any] = lambda x: ...,
) -> Tuple[str, int]:
    """Wait until download URL can be generated.
    Returns a tuple with two elements:
        1. the download url
        2. the file size in bytes
    """

    # get the download_url, wait if needed
    wait_time = 0
    while wait_time < max_wait_time:
        try:
            response = download_api_call(api_url, file_id)
        except BadResponseCodeError as error:
            logger("The request was invalid and returnd a wrong HTTP status code.")
            raise error
        except RequestFailedError as error:
            logger("The request has failed.")
            raise error

        if response[0] is not None:
            download_url: str = response[0]
            file_size: int = response[1]
            return (download_url, file_size)

        retry_time: int = response[2]

        wait_time += retry_time
        logger(f"File staging, will try to download again in {retry_time} seconds")
        sleep(retry_time)

    raise MaxWaitTimeExceeded(max_wait_time)
