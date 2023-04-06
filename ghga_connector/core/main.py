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

"""Main domain logic."""

import os
from pathlib import Path
from queue import Queue

import crypt4gh.keys

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls import (
    UploadStatus,
    await_download_url,
    check_url,
    get_download_urls,
    get_file_header_envelope,
    get_part_upload_urls,
    patch_multipart_upload,
    start_multipart_upload,
)
from ghga_connector.core.file_operations import (
    Crypt4GHEncryptor,
    calc_part_ranges,
    download_file_parts,
    is_file_encrypted,
    read_file_parts,
    upload_file_part,
)
from ghga_connector.core.message_display import AbstractMessageDisplay


def upload(  # noqa C901, pylint: disable=too-many-statements,too-many-branches
    *,
    api_url: str,
    file_id: str,
    file_path: Path,
    message_display: AbstractMessageDisplay,
    server_pubkey: str,
    submitter_pubkey_path: Path,
    submitter_private_key_path: Path,
) -> None:
    """
    Core command to upload a file. Can be called by CLI, GUI, etc.
    """

    if not submitter_pubkey_path.is_file():
        message_display.failure(f"The file {submitter_pubkey_path} does not exist.")
        raise exceptions.PubKeyFileDoesNotExistError(pubkey_path=submitter_pubkey_path)

    if not submitter_private_key_path.is_file():
        message_display.failure(
            f"The file {submitter_private_key_path} does not exist."
        )
        raise exceptions.PrivateKeyFileDoesNotExistError(
            private_key_path=submitter_private_key_path
        )

    if not file_path.is_file():
        message_display.failure(f"The file {file_path} does not exist.")
        raise exceptions.FileDoesNotExistError(file_path=file_path)

    if is_file_encrypted(file_path):
        message_display.failure(
            f"The file {file_path} is already Crypt4GH encrypted."
            + "\nThis is currently not supported."
            + " Please provide your data without Crypt4GH encryption."
        )
        raise exceptions.FileAlreadyEncryptedError(file_path=file_path)

    if not check_url(api_url):
        message_display.failure(f"The url {api_url} is currently not reachable.")
        raise exceptions.ApiNotReachableError(api_url=api_url)

    try:
        upload_id, part_size = start_multipart_upload(
            api_url=api_url, file_id=file_id, pubkey_path=submitter_pubkey_path
        )
    except exceptions.NoUploadPossibleError as error:
        message_display.failure(
            f"This user can't start a multipart upload for the file_id '{file_id}'"
        )
        raise error
    except exceptions.UploadNotRegisteredError as error:
        message_display.failure(
            f"The pending upload for file '{file_id}' does not exist."
        )
        raise error
    except exceptions.UserHasNoUploadAccessError as error:
        message_display.failure(
            f"The user is not registered as a Data Submitter for the file with id '{file_id}'."
        )
        raise error
    except exceptions.FileNotRegisteredError as error:
        message_display.failure(f"The file with the id {file_id} is not registered.")
        raise error
    except exceptions.BadResponseCodeError as error:
        message_display.failure(
            "The request was invalid and returnd a wrong HTTP status code."
        )
        raise error
    except exceptions.CantChangeUploadStatusError as error:
        message_display.failure(f"The file with id '{file_id}' was already uploaded.")
        raise error
    except exceptions.RequestFailedError as error:
        message_display.failure("The request to start a multipart upload has failed.")
        raise error

    encryptor = Crypt4GHEncryptor(
        server_pubkey=server_pubkey,
        submitter_private_key_path=submitter_private_key_path,
    )

    encrypted_file_path = encryptor.encrypt_file(file_path=file_path)

    try:
        upload_file_parts(
            api_url=api_url,
            upload_id=upload_id,
            part_size=part_size,
            file_path=Path(encrypted_file_path),
        )
    except exceptions.MaxRetriesReachedError as error:
        message_display.failure(
            "The upload has failed too many times. The upload was aborted."
        )
        raise error
    finally:
        # remove temporary encrypted file
        os.remove(encrypted_file_path)

    try:
        patch_multipart_upload(
            api_url=api_url,
            upload_id=upload_id,
            upload_status=UploadStatus.UPLOADED,
        )
    except exceptions.BadResponseCodeError as error:
        message_display.failure(
            f"The request to confirm the upload with id {upload_id} was invalid."
        )
        raise error
    except exceptions.RequestFailedError as error:
        message_display.failure(f"Confirming the upload with id {upload_id} failed.")
        raise error
    message_display.success(f"File with id '{file_id}' has been successfully uploaded.")


def upload_file_parts(
    *,
    api_url: str,
    upload_id: str,
    part_size: int,
    file_path: Path,
) -> None:
    """
    Uploads a file using a specific upload id via uploading all its parts.
    """

    with open(file_path, "rb") as file:
        file_parts = read_file_parts(file, part_size=part_size)
        upload_urls = get_part_upload_urls(api_url=api_url, upload_id=upload_id)

        for part, upload_url in zip(file_parts, upload_urls):
            upload_file_part(presigned_url=upload_url, part=part)


def download(  # pylint: disable=too-many-arguments
    *,
    api_url: str,
    file_id: str,
    output_dir: Path,
    part_size: int,
    message_display: AbstractMessageDisplay,
    max_wait_time: int,
    pubkey_path: Path,
) -> None:
    """
    Core command to download a file. Can be called by CLI, GUI, etc.
    """

    if not os.path.isdir(output_dir):
        message_display.failure(f"The directory {output_dir} does not exist.")
        raise exceptions.DirectoryDoesNotExistError(output_dir=output_dir)

    if not os.path.isfile(pubkey_path):
        message_display.failure(f"The file {pubkey_path} does not exist.")
        raise exceptions.PubKeyFileDoesNotExistError(pubkey_path=pubkey_path)

    if not check_url(api_url):
        message_display.failure(f"The url {api_url} is currently not reachable.")
        raise exceptions.ApiNotReachableError(api_url=api_url)

    public_key = crypt4gh.keys.get_public_key(pubkey_path)

    # check output file
    output_file = os.path.join(output_dir, file_id)
    if os.path.isfile(output_file):
        message_display.failure(f"The file {output_file} already exists.")
        raise exceptions.FileAlreadyExistsError(output_file=output_file)

    # stage download and get file size
    download_url_tuple = await_download_url(
        api_url=api_url,
        file_id=file_id,
        max_wait_time=max_wait_time,
        message_display=message_display,
    )

    # get file header envelope
    try:
        envelope = get_file_header_envelope(
            file_id=file_id,
            api_url=api_url,
            public_key=public_key,
        )
    except (
        exceptions.FileNotRegisteredError,
        exceptions.EnvelopeNotFoundError,
        exceptions.ExternalApiError,
    ) as error:
        message_display.failure(
            f"The request to get an envelope for file {file_id} failed."
        )
        raise error

    # perform the download
    try:
        download_parts(
            envelope=envelope,
            file_id=file_id,
            api_url=api_url,
            output_file=output_file,
            part_size=part_size,
            file_size=download_url_tuple[1],
        )
    except exceptions.MaxRetriesReachedError as error:
        # Remove file, if the download failed.
        os.remove(output_file)
        raise error
    except exceptions.NoS3AccessMethodError as error:
        message_display.failure(
            f"The request to return information for file {file_id}"
            + " did not return an S3 access method."
        )
        os.remove(output_file)
        raise error

    message_display.success(
        f"File with id '{file_id}' has been successfully downloaded."
    )


def download_parts(
    *,
    max_concurrent_downloads: int = 5,
    max_queue_size: int = 10,
    part_size: int,
    file_size: int,
    api_url: str,
    file_id: str,
    output_file: str,
    envelope: bytes,
):
    """
    Downloads a file from the given URL using multiple threads and saves it to a file.

    :param max_concurrent_downloads: Maximum number of parallel downloads.
    :param max_queue_size: Maximum size of the queue.
    :param part_size: Size of each part to download.
    """

    # Split the file into parts based on the part size
    part_ranges = calc_part_ranges(part_size=part_size, total_file_size=file_size)

    # Create a queue object to store downloaded parts
    queue: Queue = Queue(maxsize=max_queue_size)

    # Get the download urls
    download_urls = get_download_urls(
        api_url=api_url,
        file_id=file_id,
    )

    # Download the file parts in parallel
    download_file_parts(
        max_concurrent_downloads=max_concurrent_downloads,
        queue=queue,
        part_ranges=part_ranges,
        download_urls=download_urls,
    )

    # Write the downloaded parts to a file
    with open(output_file, "wb") as file:
        downloaded_size = 0
        # put envelope in file
        file.write(envelope)
        while downloaded_size < file_size:
            start, part = queue.get()
            file.seek(start)
            file.write(part)
            downloaded_size += len(part)
            queue.task_done()
