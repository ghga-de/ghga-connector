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

""" CLI-specific wrappers around core functions."""


import typer

from ghga_connector.core import (
    MAX_RETRIES,
    AbstractMessageDisplay,
    MessageColors,
    download_core,
    upload_core,
)

DEFAULT_PART_SIZE = 16 * 1024 * 1024


class CLIMessageDisplay(AbstractMessageDisplay):
    """
    Command line writer message display implementation,
    using different color based on information type
    """

    def display(self, message: str):
        """
        Write message with default color to stdout
        """
        typer.secho(message, fg=MessageColors.DEFAULT)

    def success(self, message: str):
        """
        Write message to stdout representing information about a successful operation
        """
        typer.secho(message, fg=MessageColors.SUCCESS)

    def failure(self, message: str):
        """
        Write message to stderr representing information about a failed operation
        """
        typer.secho(message, fg=MessageColors.FAILURE, err=True)


cli = typer.Typer()
message_display: AbstractMessageDisplay = CLIMessageDisplay()


@cli.command()
def upload(  # noqa C901
    api_url: str = typer.Option(..., help="Url to the upload contoller"),
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    file_path: str = typer.Option(..., help="The path to the file to upload"),
    max_retries: int = typer.Argument(
        default=MAX_RETRIES,
        help="Number of times to retry failed part uploads",
    ),
):
    """
    Command to upload a file
    """

    try:
        upload_core(
            api_url=api_url,
            file_id=file_id,
            file_path=file_path,
            max_retries=max_retries,
            message_display=message_display,
        )
    except Exception as error:
        raise typer.Abort() from error


@cli.command()
def download(  # pylint: disable=too-many-arguments
    api_url: str = typer.Option(..., help="Url to the DRS3"),
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    output_dir: str = typer.Option(
        ..., help="The directory to put the downloaded file"
    ),
    max_wait_time: int = typer.Argument(
        60,
        help="Maximal time in seconds to wait before quitting without a download.",
    ),
    part_size: int = typer.Argument(
        DEFAULT_PART_SIZE, help="Part size of the downloaded chunks."
    ),
    max_retries: int = typer.Argument(
        default=MAX_RETRIES,
        help="Number of times to retry failed part downloads",
    ),
):
    """
    Command to download a file
    """

    try:
        download_core(
            api_url=api_url,
            file_id=file_id,
            output_dir=output_dir,
            max_wait_time=max_wait_time,
            part_size=part_size,
            max_retries=max_retries,
            message_display=message_display,
        )
    except Exception as error:
        raise typer.Abort() from error
