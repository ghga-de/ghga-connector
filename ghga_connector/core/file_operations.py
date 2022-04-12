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
Contains Calls of the Presigned URLs in order to Up- and Download Files
"""

import os

import pycurl

from .exceptions import BadResponseCodeError, RequestFailedError


def download_file(download_url, output_file_path):
    """Download File"""

    with open(output_file_path, "wb") as file:
        curl = pycurl.Curl()
        curl.setopt(curl.URL, download_url)
        curl.setopt(curl.WRITEDATA, file)
        try:
            curl.perform()
        except pycurl.error as pycurl_error:
            raise RequestFailedError(download_url) from pycurl_error

        status_code = curl.getinfo(pycurl.RESPONSE_CODE)
        curl.close()

    if status_code == 200:
        return

    raise BadResponseCodeError(url=download_url, response_code=status_code)


def upload_file(upload_url, upload_file_path):
    """Upload File"""

    file_size = os.path.getsize(upload_file_path)
    curl = pycurl.Curl()
    curl.setopt(curl.URL, upload_url)
    curl.setopt(curl.POSTFIELDS, upload_file_path)
    curl.setopt(curl.CURLOPT_POSTFIELDSIZE, file_size)
    try:
        curl.perform()
    except pycurl.error as pycurl_error:
        raise RequestFailedError(upload_url) from pycurl_error

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    curl.close()

    if status_code == 200:
        return

    raise BadResponseCodeError(url=upload_url, response_code=status_code)
