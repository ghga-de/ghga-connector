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
"""Module dealing with intermediate upload path abstractions"""

import math
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Iterator

import crypt4gh.lib

from ghga_connector.core import exceptions
from ghga_connector.core.file_operations import Crypt4GHEncryptor
from ghga_connector.core.message_display import AbstractMessageDisplay


class UploadStatus(str, Enum):
    """
    Enum for the possible statuses of an upload attempt.
    """

    ACCEPTED = "accepted"
    CANCELLED = "cancelled"
    FAILED = "failed"
    PENDING = "pending"
    REJECTED = "rejected"
    UPLOADED = "uploaded"


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
        """
        Get all file metadata
        """

    @abstractmethod
    async def get_part_upload_url(self, *, part_no: int) -> str:
        """
        Get a presigned url to upload a specific part
        """

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
        """
        Get details on a specific upload
        """

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


class ChunkedUploader:
    """Handler class dealing with upload functionality"""

    def __init__(
        self,
        encryptor: Crypt4GHEncryptor,
        file_id: str,
        file_path: Path,
        part_size: int,
        uploader: UploaderBase,
    ) -> None:
        self._encrypted_file_size = 0
        self._encryptor = encryptor
        self._file_id = file_id
        self._input_path = file_path
        self._part_size = part_size
        self._unencrypted_file_size = file_path.stat().st_size
        self._uploader = uploader

    async def encrypt_and_upload(self):
        """Delegate encryption and perform multipart upload"""

        # compute encrypted_file_size
        num_segments = math.ceil(
            self._unencrypted_file_size / crypt4gh.lib.SEGMENT_SIZE
        )
        expected_encrypted_size = (
            self._unencrypted_file_size + num_segments * crypt4gh.lib.CIPHER_DIFF
        )

        with self._input_path.open("rb") as file:
            for part_number, part in enumerate(
                self._encryptor.process_file(file=file), start=1
            ):
                upload_url = await self._uploader.get_part_upload_url(
                    part_no=part_number
                )
                await self._uploader.upload_file_part(
                    presigned_url=upload_url, part=part
                )
            encrypted_file_size = self._encryptor.get_encrypted_size()
            if expected_encrypted_size != encrypted_file_size:
                raise exceptions.EncryptedSizeMismatch(
                    actual_encrypted_size=encrypted_file_size,
                    expected_encrypted_size=expected_encrypted_size,
                )


async def run_upload(  # pylint: disable=too-many-arguments
    file_path: Path,
    message_display: AbstractMessageDisplay,
    private_key_path: Path,
    server_public_key: str,
    uploader: UploaderBase,
):
    """
    Initialize httpx.client and Uploader and delegate to function performing the actual
    upload
    """
    try:
        await uploader.start_multipart_upload()
    except (
        exceptions.BadResponseCodeError,
        exceptions.FileNotRegisteredError,
        exceptions.NoUploadPossibleError,
        exceptions.UploadNotRegisteredError,
        exceptions.UserHasNoUploadAccessError,
    ) as error:
        raise error
    except exceptions.CantChangeUploadStatusError as error:
        message_display.failure(f"The file with id '{file_id}' was already uploaded.")
        raise error
    except exceptions.RequestFailedError as error:
        message_display.failure("The request to start a multipart upload has failed.")
        raise error

    try:
        await execute_upload(
            uploader=uploader,
            file_path=file_path,
            private_key_path=private_key_path,
            server_public_key=server_public_key,
        )
    except exceptions.ConnectionFailedError as error:
        message_display.failure("The upload failed too many times and was aborted.")
        raise error

    try:
        await uploader.finish_multipart_upload()
    except exceptions.BadResponseCodeError as error:
        message_display.failure(
            f"The request to confirm the upload with id '{upload_id}' was invalid."
        )
        raise error
    except exceptions.RequestFailedError as error:
        message_display.failure(f"Confirming the upload with id '{upload_id}' failed.")
        raise error


async def execute_upload(
    uploader: UploaderBase,
    file_path: Path,
    private_key_path: Path,
    server_public_key: str,
):
    """
    Create encryptor and chunked_uploader instances for a given uploaded and call the
    method performing the actual encryption and download
    """
    encryptor = Crypt4GHEncryptor(
        part_size=part_size,
        private_key_path=private_key_path,
        server_public_key=server_public_key,
    )
    chunked_uploader = ChunkedUploader(
        encryptor=encryptor, file_path=file_path, uploader=uploader
    )
    await chunked_uploader.encrypt_and_upload()
