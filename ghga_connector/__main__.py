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

"""Entrypoint of the package"""


from os import path

import pycurl
import typer

app = typer.Typer()


@app.command()
def upload(
    api_url: str = typer.Option(..., help="Url to the upload contoller"),
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    file_path: str = typer.Option(..., help="The path to the file to upload"),
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

    typer.echo(f"Upload of file with id '{file_id}' has been completed.")


@app.command()
def download(
    api_url: str = typer.Option(..., help="Url to the DRS3"),
    file_id: str = typer.Option(..., help="The id if the file to upload"),
    output_dir: str = typer.Option(
        ..., help="The directory to put the downloaded file"
    ),
):
    """
    Command to download a file
    """
    if not path.isdir(output_dir):
        typer.echo(f"The directory {output_dir} does not exist.")
        raise typer.Abort()

    if not check_url(api_url):
        typer.echo(f"The url {api_url} is currently not reachable.")
        raise typer.Abort()

    typer.echo(f"Download of file with id '{file_id}' has been completed.")


def check_url(api_url, wait_time=1000) -> bool:
    """
    Checks, if an url is reachable within a certain time
    """
    curl = pycurl.Curl()
    curl.setopt(curl.URL, api_url)
    curl.setopt(curl.CONNECTTIMEOUT_MS, wait_time)
    try:
        curl.perform_rb()
    except pycurl.error:
        return False
    return True


if __name__ == "__main__":
    app()
