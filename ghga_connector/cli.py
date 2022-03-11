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

import json
import urllib
from io import BytesIO
from os import path

import pycurl
import typer

from ghga_connector.core import check_url

cli = typer.Typer()


@cli.command()
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

    # build curl URL
    params = {"presigned_post": file_id}
    url = api_url + urllib.parse.urlencode(params)

    # Make function call to get upload url
    curl = pycurl.Curl()
    data = BytesIO()
    curl.setopt(pycurl.URL, url)
    curl.setopt(pycurl.WRITEFUNCTION, data.write)

    curl.setopt(
        pycurl.HTTPHEADER,
        ["Accept: application/json", "Content-Type: application/json"],
    )
    curl.setopt(pycurl.GET, 1)
    try:
        curl.perform()
    except pycurl.error:
        typer.Abort()

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)

    if status_code != 200:
        typer.Abort()

    dictionary = json.loads(data.getvalue())
    response_url = dictionary[0]

    typer.echo(f"File with id '{file_id}' can be uploaded via {response_url}.")


@cli.command()
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

    # build curl URL
    params = {"objects": file_id}
    url = api_url + urllib.parse.urlencode(params)

    # Make function call to get upload url
    curl = pycurl.Curl()
    data = BytesIO()
    curl.setopt(pycurl.URL, url)
    curl.setopt(pycurl.WRITEFUNCTION, data.write)
    curl.setopt(
        pycurl.HTTPHEADER,
        ["Accept: application/json", "Content-Type: application/json"],
    )
    curl.setopt(pycurl.GET, 1)
    try:
        curl.perform()
    except pycurl.error:
        typer.Abort()

    status_code = curl.getinfo(pycurl.RESPONSE_CODE)

    if status_code != 200:
        typer.Abort()

    dictionary = json.loads(data.getvalue())
    response_url = dictionary["access_methods"]["s3"]["access_url"]

    typer.echo(f"File with id '{file_id}' can be downloaded via {response_url}.")
