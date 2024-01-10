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
from pathlib import Path

import crypt4gh.lib
from httpx import AsyncClient

from ghga_connector.core import exceptions
from ghga_connector.core.file_operations import Crypt4GHEncryptor, Encryptor
from ghga_connector.core.upload.api_calls import Uploader, UploaderBase


class ChunkedUploader:
    """Handler class dealing with upload functionality"""

    def __init__(  # noqa: PLR0913
        self,
        *,
        encryptor: Encryptor,
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


async def run_upload(  # noqa: PLR0913
    api_url: str,
    client: AsyncClient,
    file_id: str,
    file_path: Path,
    my_private_key_path: Path,
    my_public_key_path: Path,
    part_size: int,
    server_public_key: str,
):
    """Initialize client and uploader and delegate to function performing the actual upload"""
    uploader = uploader = Uploader(
        api_url=api_url,
        client=client,
        file_id=file_id,
        public_key_path=my_public_key_path,
    )
    encryptor = Crypt4GHEncryptor(
        part_size=part_size,
        private_key_path=my_private_key_path,
        server_public_key=server_public_key,
    )
    chunked_uploader = ChunkedUploader(
        encryptor=encryptor,
        file_path=file_path,
        file_id=file_id,
        part_size=part_size,
        uploader=uploader,
    )

    try:
        await uploader.start_multipart_upload()
    except (
        exceptions.BadResponseCodeError,
        exceptions.FileNotRegisteredError,
        exceptions.NoUploadPossibleError,
        exceptions.RequestFailedError,
        exceptions.UploadNotRegisteredError,
        exceptions.UserHasNoUploadAccessError,
    ) as error:
        raise exceptions.StartUploadError() from error

    try:
        await chunked_uploader.encrypt_and_upload()
    except exceptions.ConnectionFailedError as error:
        raise error

    try:
        await uploader.finish_multipart_upload()
    except exceptions.BadResponseCodeError as error:
        raise exceptions.FinalizeUploadError(
            cause="The request to confirm the upload was invalid."
        ) from error
    except exceptions.RequestFailedError as error:
        raise exceptions.FinalizeUploadError(
            cause="Confirming the upload failed."
        ) from error
