# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Batch processing for the upload process"""

import logging

from pydantic import SecretBytes

from ghga_connector.core import CLIMessageDisplay
from ghga_connector.core.crypt.encryption import Crypt4GHEncryptor
from ghga_connector.core.uploading.api_calls import UploadClient
from ghga_connector.core.uploading.structs import FileInfoForUpload
from ghga_connector.core.uploading.uploader import Uploader

log = logging.getLogger(__name__)


async def upload_files_from_list(
    *,
    upload_client: UploadClient,
    file_info_list: list[FileInfoForUpload],
    my_private_key: SecretBytes,
    max_concurrent_uploads: int,
):
    """Upload all files in the provided list of file paths.

    If the user cancels the upload, e.g. via CTRL+C, or if an unexpected error occurs,
    the in progress file will be cancelled and the upload process halted.
    """
    CLIMessageDisplay.display(f"Starting batch upload of {len(file_info_list)} files")
    for file_info in file_info_list:
        encryptor = Crypt4GHEncryptor(
            part_size=file_info.part_size,  # this will be the adjusted part size
            my_private_key=my_private_key,
            file_size=file_info.decrypted_size,
        )
        uploader = Uploader(
            upload_client=upload_client,
            encryptor=encryptor,
            file_info=file_info,
            max_concurrent_uploads=max_concurrent_uploads,
        )
        log.info("Initializing upload for %s", file_info.alias)
        log.debug("Full file path is %s", str(file_info.path.resolve()))
        file_id = await uploader.initiate_file_upload()
        log.info(
            "File upload successfully initialized for %s."
            + " The generated file ID is %s.",
            file_info.alias,
            file_id,
        )

        log.info("Encrypting and uploading %s", file_info.alias)
        try:
            await uploader.upload_file()
        except KeyboardInterrupt:
            # User cancellation is handled here
            CLIMessageDisplay.failure(
                f"User aborted upload for {file_info.alias}, (file ID {file_id}), deleting."
            )
            await uploader.delete_file()
            CLIMessageDisplay.success(
                f"File upload for {file_info.alias} successfully cancelled."
                + "\nUpload process stopped. If applicable, any previously completed"
                + " file uploads remain uploaded."
            )
        except BaseException as err:
            # All other errors are handled here
            CLIMessageDisplay.failure(str(err))
            CLIMessageDisplay.failure(
                f"Failed to upload {file_info.alias}, (file ID {file_id}), deleting."
            )
            await uploader.delete_file()
            CLIMessageDisplay.display(
                f"File upload for {file_info.alias} was cancelled."
            )
        else:
            # This is the success case
            CLIMessageDisplay.success(f"Successfully uploaded {file_info.alias}.")
