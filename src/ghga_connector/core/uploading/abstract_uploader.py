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
from abc import ABC, abstractmethod
from collections.abc import Iterator

from ghga_connector.core.uploading.dataclasses import UploadStatus


class UploaderBase(ABC):
    """
    Class bundling functionality calling Upload Controller Service to initiate and
    manage an ongoing upload
    """

    @abstractmethod
    async def start_multipart_upload(self):
        """Start multipart upload"""

    @abstractmethod
    async def finish_multipart_upload(self):
        """Complete or clean up multipart upload"""

    @abstractmethod
    async def get_file_metadata(self) -> dict[str, str]:
        """Get all file metadata"""

    @abstractmethod
    async def get_part_upload_url(self, *, part_no: int) -> str:
        """Get a presigned url to upload a specific part"""

    @abstractmethod
    def get_part_upload_urls(
        self,
        *,
        from_part: int = 1,
        get_url_func=get_part_upload_url,
    ) -> Iterator[str]:
        """
        For a specific mutli-part upload identified by the `upload_id`, it returns an
        iterator to iterate through file parts and obtain the corresponding upload urls.

        By default it start with the first part but you may also start from a specific part
        in the middle of the file using the `from_part` argument. This might be useful to
        resume an interrupted upload process.

        Please note: the upload corresponding to the `upload_id` must have already been
        initiated.

        `get_url_func` only for testing purposes.
        """

    @abstractmethod
    async def get_upload_info(self) -> dict[str, str]:
        """Get details on a specific upload"""

    @abstractmethod
    async def patch_multipart_upload(self, *, upload_status: UploadStatus) -> None:
        """
        Set the status of a specific upload attempt.
        The API accepts "uploaded" or "accepted",
        if the upload_id is currently set to "pending"
        """

    @abstractmethod
    async def upload_file_part(self, *, presigned_url: str, part: bytes) -> None:
        """Upload File"""
