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

from pathlib import Path
from typing import List

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls import Downloader, WorkPackageAccessor, check_url
from ghga_connector.core.client import httpx_client
from ghga_connector.core.download import run_download
from ghga_connector.core.file_operations import Crypt4GHDecryptor, is_file_encrypted
from ghga_connector.core.message_display import AbstractMessageDisplay
from ghga_connector.core.upload import run_upload


async def upload(  # noqa C901, pylint: disable=too-many-statements,too-many-branches
    *,
    api_url: str,
    file_id: str,
    file_path: Path,
    message_display: AbstractMessageDisplay,
    server_public_key: str,
    my_public_key_path: Path,
    my_private_key_path: Path,
) -> None:
    """
    Core command to upload a file. Can be called by CLI, GUI, etc.
    """

    if not my_public_key_path.is_file():
        raise exceptions.PubKeyFileDoesNotExistError(pubkey_path=my_public_key_path)

    if not my_private_key_path.is_file():
        raise exceptions.PrivateKeyFileDoesNotExistError(
            private_key_path=my_private_key_path
        )

    if not file_path.is_file():
        raise exceptions.FileDoesNotExistError(file_path=file_path)

    if is_file_encrypted(file_path):
        raise exceptions.FileAlreadyEncryptedError(file_path=file_path)

    if not check_url(api_url):
        raise exceptions.ApiNotReachableError(api_url=api_url)

    await run_upload(
        api_url=api_url,
        file_id=file_id,
        file_path=file_path,
        message_display=message_display,
        private_key_path=my_private_key_path,
        public_key_path=my_public_key_path,
        server_public_key=server_public_key,
    )

    message_display.success(f"File with id '{file_id}' has been successfully uploaded.")


def download(  # pylint: disable=too-many-arguments, too-many-locals # noqa: C901, R0914
    *,
    api_url: str,
    output_dir: Path,
    part_size: int,
    message_display: AbstractMessageDisplay,
    max_wait_time: int,
    work_package_accessor: WorkPackageAccessor,
    file_id: str,
    file_extension: str = "",
) -> None:
    """
    Core command to download a file. Can be called by CLI, GUI, etc.
    """

    if not check_url(api_url):
        raise exceptions.ApiNotReachableError(api_url=api_url)

    # construct file name with suffix, if given
    file_name = f"{file_id}"
    if file_extension:
        file_name = f"{file_id}{file_extension}"

    # check output file
    output_file = output_dir / f"{file_name}.c4gh"
    if output_file.exists():
        raise exceptions.FileAlreadyExistsError(output_file=str(output_file))

    # with_suffix() might overwrite existing suffixes, do this instead
    output_file_ongoing = output_file.parent / (output_file.name + ".part")
    if output_file_ongoing.exists():
        output_file_ongoing.unlink()

    with httpx_client() as client:
        downloader = Downloader(
            client=client, file_id=file_id, work_package_accessor=work_package_accessor
        )
        run_download(
            downloader=downloader,
            max_wait_time=max_wait_time,
            message_display=message_display,
            output_file_ongoing=output_file_ongoing,
            part_size=part_size,
        )

    # rename fully downloaded file
    if output_file.exists():
        raise exceptions.DownloadFinalizationError(file_path=output_file)
    output_file_ongoing.rename(output_file)

    message_display.success(
        f"File with id '{file_id}' has been successfully downloaded."
    )


def get_wps_token(max_tries: int, message_display: AbstractMessageDisplay) -> List[str]:
    """
    Expect the work package id and access token as a colon separated string
    The user will have to input this manually to avoid it becoming part of the
    command line history.
    """
    for _ in range(max_tries):
        work_package_string = input(
            "Please paste the complete download token "
            + "that you copied from the GHGA data portal: "
        )
        work_package_parts = work_package_string.split(":")
        if not (
            len(work_package_parts) == 2
            and 20 <= len(work_package_parts[0]) < 40
            and 80 <= len(work_package_parts[1]) < 120
        ):
            message_display.display(
                "Invalid input. Please enter the download token "
                + "you got from the GHGA data portal unaltered."
            )
            continue
        return work_package_parts
    raise exceptions.InvalidWorkPackageToken(tries=max_tries)


def decrypt_file(
    input_file: Path, output_file: Path, decryption_private_key_path: Path
):
    """Delegate decryption of a file Crypt4GH"""
    decryptor = Crypt4GHDecryptor(decryption_key_path=decryption_private_key_path)
    decryptor.decrypt_file(input_path=input_file, output_path=output_file)
