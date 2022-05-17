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

from os import path

import typer

from ghga_connector.core import (
    BadResponseCodeError,
    RequestFailedError,
    UploadStatus,
    await_download_url,
    check_url,
    download_file_part,
    initiate_multipart_upload,
    part_upload,
    patch_multipart_upload,
    restart_multipart_upload,
    upload_file_part,
)
from ghga_connector.core.exceptions import NoUploadPossibleError

DEFAULT_PART_SIZE = 16 * 1024 * 1024


class DirectoryNotExist(RuntimeError):
    """Thrown, when the specified directory does not exist."""

    def __init__(self, output_dir: str):
        message = f"The directory {output_dir} does not exist."
        super().__init__(message)


class ApiNotReachable(RuntimeError):
    """Thrown, when the api is not reachable."""

    def __init__(self, api_url: str):
        message = f"The url {api_url} is currently not reachable."
        super().__init__(message)


cli = typer.Typer()


@cli.command()
def upload(  # noqa C901, pylint: disable=too-many-branches
    api_url: str = typer.Option(..., help="Url to the upload contoller"),
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    file_path: str = typer.Option(..., help="The path to the file to upload"),
    max_retries: int = typer.Argument(
        "3", help="Maximum number of tries to upload a single file part."
    ),
):
    """
    Command to upload a file
    """
    if not path.isfile(file_path):
        typer.echo(f"The file {file_path} does not exist.")
        raise typer.Abort()

    if not check_url(api_url):
        typer.echo(f"The url {api_url} is currently not reachable.")
        raise typer.Abort()

    try:
        upload_id, part_size = initiate_multipart_upload(
            api_url=api_url, file_id=file_id
        )
    except NoUploadPossibleError as error:
        # see if there is an open upload
        upload_id, part_size = restart_multipart_upload(
            api_url=api_url, file_id=file_id, error=error
        )

    except BadResponseCodeError as error:
        typer.echo("The request was invalid and returnd a wrong HTTP status code.")
        raise typer.Abort() from error
    except RequestFailedError as error:
        typer.echo("The request has failed.")
        raise typer.Abort() from error

    file_size = path.getsize(file_path)

    part_no = 1
    part_offset = 0

    while part_offset < file_size:

        # For 0 retries, we still try the first time
        for retries in range(0, max_retries + 1):
            presigned_post_url = part_upload(
                api_url=api_url, upload_id=upload_id, part_no=part_no
            )

            # Upload File
            try:
                upload_file_part(
                    presigned_post_url=presigned_post_url,
                    upload_file_path=file_path,
                    part_offset=part_offset,
                    part_size=part_size,
                )
                break
            except BadResponseCodeError as error:
                typer.echo(
                    "The part upload request was invalid and returnd a wrong HTTP status code."
                )
                if retries > max_retries - 1:
                    raise error
            except RequestFailedError as error:
                typer.echo("The part upload request has failed.")
                if retries > max_retries - 1:
                    raise error

        part_offset += part_size
        part_no += 1

    try:
        patch_multipart_upload(
            api_url=api_url,
            upload_id=upload_id,
            upload_status=UploadStatus.UPLOADED,
        )
    except BadResponseCodeError as error:
        typer.echo(
            f"The request to confirm the upload with id {upload_id} was invalid."
        )
        raise typer.Abort() from error
    except RequestFailedError as error:
        typer.echo(f"Confirming the upload with id {upload_id} failed.")
        raise typer.Abort() from error
    typer.echo(f"File with id '{file_id}' has been successfully uploaded.")


@cli.command()
def download(  # noqa C901, pylint: disable=too-many-arguments, too-many-branches
    api_url: str = typer.Option(..., help="Url to the DRS3"),
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    output_dir: str = typer.Option(
        ..., help="The directory to put the downloaded file"
    ),
    max_wait_time: int = typer.Argument(
        "60",
        help="Maximal time in seconds to wait before quitting without a download. ",
    ),
    part_size: int = typer.Argument(
        DEFAULT_PART_SIZE, help="Part size of the downloaded chunks."
    ),
    max_retries: int = typer.Argument(
        "3", help="Maximum number of tries to download a single file part."
    ),
):
    """
    Command to download a file
    """
    if not path.isdir(output_dir):
        raise DirectoryNotExist(output_dir)

    if not check_url(api_url):
        raise ApiNotReachable(api_url)

    download_url, file_size = await_download_url(
        api_url=api_url, file_id=file_id, max_wait_time=max_wait_time, logger=typer.echo
    )

    # perform the download:
    output_file = path.join(output_dir, file_id)

    part_offset = 0

    while part_offset < file_size:

        # For 0 retries, we still try the first time
        for retries in range(0, max_retries + 1):
            try:
                download_file_part(
                    download_url=download_url,
                    output_file_path=output_file,
                    part_offset=part_offset,
                    part_size=part_size,
                    file_size=file_size,
                )

            except BadResponseCodeError as error:
                typer.echo(
                    "The download request was invalid and returnd a wrong HTTP status code."
                )
                if retries > max_retries - 1:
                    raise error
            except RequestFailedError as error:
                typer.echo("The download request has failed.")
                if retries > max_retries - 1:
                    raise error

        # If part download was successfull, go to the next part
        part_offset += part_size

    typer.echo(f"File with id '{file_id}' has been successfully downloaded.")
