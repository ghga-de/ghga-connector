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

""" CLI-specific wrappers around core functions."""

import os
from pathlib import Path

import crypt4gh.keys
import typer

from ghga_connector import core
from ghga_connector.config import Config

CONFIG = Config()  # will be patched for testing


class CLIMessageDisplay(core.AbstractMessageDisplay):
    """
    Command line writer message display implementation,
    using different color based on information type
    """

    def display(self, message: str):
        """
        Write message with default color to stdout
        """
        typer.secho(message, fg=core.MessageColors.DEFAULT)

    def success(self, message: str):
        """
        Write message to stdout representing information about a successful operation
        """
        typer.secho(message, fg=core.MessageColors.SUCCESS)

    def failure(self, message: str):
        """
        Write message to stderr representing information about a failed operation
        """
        typer.secho(message, fg=core.MessageColors.FAILURE, err=True)


cli = typer.Typer()


@cli.command()
def upload(  # noqa C901
    *,
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    file_path: Path = typer.Option(..., help="The path to the file to upload"),
    submitter_pubkey_path: Path = typer.Argument(
        "./key.pub",
        help="The path to a public key from the key pair that was announced in the "
        + "metadata. Defaults to the file key.pub in the current folder.",
    ),
    submitter_private_key_path: Path = typer.Argument(
        "./key.sec",
        help="The path to a private key from the key pair that will be used to encrypt the "
        + "crypt4gh envelope. Defaults to the file key.pub in the current folder.",
    ),
):
    """
    Command to upload a file
    """
    core.RequestsSession.configure(CONFIG.max_retries)

    core.upload(
        api_url=CONFIG.upload_api,
        file_id=file_id,
        file_path=file_path,
        message_display=CLIMessageDisplay(),
        server_pubkey=CONFIG.server_pubkey,
        submitter_pubkey_path=submitter_pubkey_path,
        submitter_private_key_path=submitter_private_key_path,
    )


@cli.command()
def download(  # pylint: disable=too-many-arguments
    *,
    output_dir: Path = typer.Option(
        ..., help="The directory to put the downloaded files into"
    ),
    submitter_pubkey_path: Path = typer.Argument(
        "./key.pub",
        help="The path to a public key from the key pair that was announced in the "
        + "metadata. Defaults to the file key.pub in the current folder.",
    ),
    submitter_private_key_path: Path = typer.Argument(
        "./key.sec",
        help="The path to a private key from the key pair that will be used to encrypt the "
        + "crypt4gh envelope. Defaults to the file key.pub in the current folder.",
    ),
):
    """
    Command to download a file
    """
    core.RequestsSession.configure(CONFIG.max_retries)
    message_display = CLIMessageDisplay()

    if not os.path.isfile(submitter_pubkey_path):
        message_display.failure(f"The file {submitter_pubkey_path} does not exist.")
        raise core.exceptions.PubKeyFileDoesNotExistError(
            pubkey_path=submitter_pubkey_path
        )

    if not output_dir.is_dir():
        message_display.failure(f"The directory {output_dir} does not exist.")
        raise core.exceptions.DirectoryDoesNotExistError(output_dir=output_dir)

    wps_info = core.get_wps_info(config=CONFIG)
    # get and compare user public keys
    announced_user_pubkey = wps_info.user_pubkey
    provided_pubkey = crypt4gh.keys.get_public_key(submitter_pubkey_path)

    if announced_user_pubkey != provided_pubkey:
        raise core.exceptions.PubkeyMismatchError()

    file_ids_with_extension = wps_info.file_ids_with_extension

    io_handler = core.CliIoHandler()
    staging_parameters = core.StagingParameters(
        api_url=CONFIG.download_api,
        file_ids_with_extension=file_ids_with_extension,
        max_wait_time=CONFIG.max_wait_time,
    )

    file_stager = core.FileStager(
        message_display=message_display,
        io_handler=io_handler,
        staging_parameters=staging_parameters,
    )
    file_stager.check_and_stage(output_dir=output_dir)

    while file_stager.file_ids_remain():
        for file_id in file_stager.get_staged():
            core.download(
                api_url=CONFIG.download_api,
                file_id=file_id,
                file_extension=file_ids_with_extension[file_id],
                output_dir=output_dir,
                max_wait_time=CONFIG.max_wait_time,
                part_size=CONFIG.part_size,
                message_display=message_display,
                pubkey_path=submitter_pubkey_path,
                private_key_path=submitter_private_key_path,
            )
        file_stager.update_staged_files()
