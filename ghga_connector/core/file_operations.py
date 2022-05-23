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

import math
from io import BufferedReader, BytesIO
from typing import Iterator

import pycurl

from .exceptions import BadResponseCodeError, RequestFailedError


def download_file_part(
    *,
    download_url: str,
    part_start: int,
    part_end: int,
) -> bytes:
    """Download the content of one file part and return it as bytes."""

    # Calculetes end of byte range of the file

    bytes_stream = BytesIO()
    curl = pycurl.Curl()

    curl.setopt(curl.RANGE, f"{part_start}-{part_end}")
    curl.setopt(curl.URL, download_url)
    curl.setopt(curl.WRITEDATA, bytes_stream)
    try:
        curl.perform()
        status_code = curl.getinfo(pycurl.RESPONSE_CODE)
    except pycurl.error as pycurl_error:
        raise RequestFailedError(download_url) from pycurl_error
    finally:
        curl.close()

    # 200, if the full file was returned (files smaller than the part size), 206 else
    if status_code in (200, 206):
        return bytes_stream.getvalue()

    raise BadResponseCodeError(url=download_url, response_code=status_code)


def calc_part_ranges(
    *, part_size: int, total_file_size: int, from_part: int = 1
) -> list[tuple]:
    """
    Calculate and return the ranges (start, end) of file parts as a list of tuples.

    By default it start with the first part but you may also start from a specific part
    in the middle of the file using the `from_part` argument. This might be useful to
    resume an interrupted reading process.
    """
    # calc the ranges for the parts that have the full part_size:
    full_part_number = math.floor(total_file_size / part_size)
    part_ranges = [
        (part_size * part_no, part_size * (part_no + 1) - 1)
        for part_no in range(from_part, full_part_number)
    ]

    if (total_file_size % part_size) > 0:
        # if the last part is smaller than the part_size, calculate it range separately:
        part_ranges.append((part_size * full_part_number, total_file_size - 1))

    return part_ranges


def download_file_parts(
    *, download_url: str, part_size: int, total_file_size: int, from_part: int = 1
) -> Iterator[bytes]:
    """
    Returns an iterator to obtain the bytes content of a file in a part by part fashion.

    By default it start with the first part but you may also start from a specific part
    in the middle of the file using the `from_part` argument. This might be useful to
    resume an interrupted reading process.
    """

    part_ranges = calc_part_ranges(
        part_size=part_size, total_file_size=total_file_size, from_part=from_part
    )

    for part_start, part_end in part_ranges:
        yield download_file_part(
            download_url=download_url, part_start=part_start, part_end=part_end
        )


def read_file_parts(
    file: BufferedReader, *, part_size: int, from_part: int = 1
) -> Iterator[bytes]:
    """
    Returns an iterator to iterate through file parts of the given size (in bytes).

    By default it start with the first part but you may also start from a specific part
    in the middle of the file using the `from_part` argument. This might be useful to
    resume an interrupted reading process.

    Please note: opening and closing of the file MUST happen outside of this function.
    """

    initial_offset = part_size * (from_part - 1)
    file.seek(initial_offset)

    while True:
        file_part = file.read(part_size)

        if len(file_part) == 0:
            return

        yield file_part


def upload_file_part(
    presigned_post_url: str,
    upload_file_path: str,
    part_offset: int,
    part_size: int,
) -> None:
    """Upload File"""

    with open(file=upload_file_path, mode="rb") as file:

        file.seek(part_offset)
        content = file.read(part_size)
        body = BytesIO(content)

        curl = pycurl.Curl()
        curl.setopt(curl.URL, presigned_post_url)

        curl.setopt(curl.UPLOAD, 1)
        curl.setopt(curl.READDATA, body)

        try:
            curl.perform()
            status_code = curl.getinfo(pycurl.RESPONSE_CODE)
        except pycurl.error as pycurl_error:
            raise RequestFailedError(presigned_post_url) from pycurl_error
        finally:
            curl.close()

        if status_code == 200:
            return

    raise BadResponseCodeError(url=presigned_post_url, response_code=status_code)
