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

"""Tests for the up- and download functions of the cli"""

import asyncio
from typing import List, Optional

import pytest
import pytest_asyncio
import typer
import uvicorn

from ghga_connector.cli import download, upload

from ..fixtures.mock_api import app


class UvicornTestServer(uvicorn.Server):
    """Uvicorn test server

    Usage:
        @pytest.fixture
        server = UvicornTestServer()
        await server.up()
        yield
        await server.down()
    """

    def __init__(self, app=app, host="127.0.0.1", port=8080):
        """Create a Uvicorn test server

        Args:
            app (FastAPI, optional): the FastAPI app. Defaults to main.app.
            host (str, optional): the host ip. Defaults to '127.0.0.1'.
            port (int, optional): the port. Defaults to PORT.
        """
        self._startup_done = asyncio.Event()
        super().__init__(config=uvicorn.Config(app, host=host, port=port))

    async def startup(self, sockets: Optional[List] = None) -> None:
        """Override uvicorn startup"""
        await super().startup(sockets=sockets)
        self.config.setup_event_loop()
        self._startup_done.set()

    async def up(self) -> None:
        """Start up server asynchronously"""
        self._serve_task = asyncio.create_task(self.serve())
        await self._startup_done.wait()

    async def down(self) -> None:
        """Shut down server asynchronously"""
        self.should_exit = True
        await self._serve_task


@pytest_asyncio.fixture
async def server():
    """Start server as test fixture and tear down after test"""
    server = UvicornTestServer()
    await server.up()
    yield
    await server.down()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api_url,file_id,output_dir,expected_exception",
    [
        (
            "https://no_real_url:8080",
            "1",
            "/workspace/example_data/file1.test",
            typer.Abort(),
        ),
        (
            "https://no_real_url:8080",
            1,
            "/workspace/example_data/file1.test",
            typer.Abort(),
        ),
        ("https://localhost:8080", "1", "/workspace/example_data/", None),
        ("https://localhost:8080", "2", "/workspace/example_data/", None),
        ("https://localhost:8080", "1m", "/workspace/example_data/", None),
        ("https://localhost:8080", "1", "/this_path/", typer.Abort()),
    ],
)
async def test_download(api_url, file_id, output_dir, expected_exception, server):

    """Test the download of a file, expects Abort, if the file was not found"""

    try:
        download(api_url, file_id, output_dir)
    except Exception as exception:
        assert exception == expected_exception

    assert expected_exception is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api_url,file_id,file_path,expected_exception",
    [
        (
            "https://no_real_url:8080",
            "1",
            "/workspace/example_data/file1.test",
            typer.Abort(),
        ),
        ("https://localhost:8080", "1", "/workspace/example_data/file1.test", None),
        ("https://localhost:8080", "2", "/workspace/example_data/file2.test", None),
        (
            "https://localhost:8080",
            "1",
            "/this_path/does_not_exist.test",
            typer.Abort(),
        ),
    ],
)
async def test_upload(api_url, file_id, file_path, expected_exception, server):

    """Test the upload of a file, expects Abort, if the file was not found"""

    try:
        upload(api_url, file_id, file_path)
    except Exception as exception:
        assert exception == expected_exception

    assert expected_exception is None
