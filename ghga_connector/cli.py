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
import sys
from dataclasses import dataclass
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
    pubkey_path: Path = typer.Argument(
        "./key.pub",
        help="The path to a public key from the key pair that will be used to encrypt the "
        + "crypt4gh envelope. Defaults to the file key.pub in the current folder.",
    ),
):
    """
    Command to download a file
    """
    core.RequestsSession.configure(CONFIG.max_retries)
    message_display = CLIMessageDisplay()

    if not os.path.isfile(pubkey_path):
        message_display.failure(f"The file {pubkey_path} does not exist.")
        raise core.exceptions.PubKeyFileDoesNotExistError(pubkey_path=pubkey_path)

    wps_info = core.get_wps_info(config=CONFIG)
    # get and compare user public keys
    announced_user_pubkey = wps_info.user_pubkey
    provided_pubkey = crypt4gh.keys.get_public_key(pubkey_path)

    if announced_user_pubkey != provided_pubkey:
        raise core.exceptions.PubkeyMismatchError()

    file_stager = FileStager(message_display=message_display)
    file_stager.check_and_stage(file_ids=wps_info.file_ids_with_ending.keys())

    while any((file_stager.staged_files, file_stager.unstaged_files)):
        for file_id in file_stager.staged_files:
            try:
                core.download(
                    api_url=CONFIG.download_api,
                    file_id=file_id,
                    output_dir=output_dir,
                    max_wait_time=CONFIG.max_wait_time,
                    part_size=CONFIG.part_size,
                    message_display=message_display,
                    pubkey_path=pubkey_path,
                )
            except:
                ...
        file_stager.update_staged_files()


@dataclass
class FileStager:
    """TODO"""

    message_display: CLIMessageDisplay

    staged_files: list[str] = []
    unstaged_files: list[str] = []

    def check_and_stage(self, file_ids: list[str]):
        """TODO"""
        unknown_ids = []

        for file_id in file_ids:
            try:
                dl_url = core.get_download_url(
                    api_url=CONFIG.download_api, file_id=file_id
                )
            except core.exceptions.BadResponseCodeError as error:
                if error.response_code == 404:
                    unknown_ids.append(file_id)
                continue

            if dl_url[0]:
                self.staged_files.append(file_id)
            else:
                self.unstaged_files.append(file_id)

        if unknown_ids:
            message = f"No download exists for the following file IDs: {' ,'.join(unknown_ids)}"
            self.message_display.failure(message)
            message = (
                "Some of the provided file IDs cannot be downloaded."
                + "\nDo you want to proceed ?\n[Yes][No]\t"
            )
            response = input(message)
            if not response.lower() == "yes":
                self.message_display.display("Aborting batch process")
                sys.exit()
            else:
                self.message_display.display("Downloading remaining files")

    def update_staged_files(self):
        """TODO"""
        self.staged_files = []
        remaining_unstaged = []
        for file_id in self.unstaged_files:
            dl_url = core.get_download_url(api_url=CONFIG.download_api, file_id=file_id)
            if dl_url[0]:
                self.staged_files.append(file_id)
            else:
                remaining_unstaged.append(file_id)
        self.unstaged_files = remaining_unstaged
