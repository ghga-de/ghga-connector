# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Module for batch processing related code"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from time import sleep, time

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls import WorkPackageAccessor, is_service_healthy
from ghga_connector.core.client import httpx_client
from ghga_connector.core.downloading.api_calls import (
    RetryResponse,
    URLResponse,
    get_download_url,
    get_file_authorization,
)
from ghga_connector.core.message_display import AbstractMessageDisplay


class InputHandler(ABC):
    """Abstract base for dealing with user input in batch processing"""

    @abstractmethod
    def get_input(self, *, message: str) -> str:
        """Handle user input."""

    @abstractmethod
    def handle_response(self, *, response: str):
        """Handle response from get_input."""


class OutputHandler(ABC):
    """Abstract base for checking existing content in a provided output location."""

    @abstractmethod
    def check_output(self, *, location: Path) -> list[str]:
        """Check for and return existing files in output location."""


@dataclass
class BatchIoHandler(ABC):
    """Convenience class to hold both input and output handlers"""

    input_handler: InputHandler
    output_handler: OutputHandler

    @abstractmethod
    def check_output(self, *, location: Path) -> list[str]:
        """Check for and return existing files in output location."""

    @abstractmethod
    def get_input(self, *, message: str) -> str:
        """User input handling."""

    @abstractmethod
    def handle_response(self, *, response: str):
        """Handle response from get_input."""


class CliInputHandler(InputHandler):
    """CLI relevant input handling"""

    def get_input(self, *, message: str) -> str:
        """Simple user input handling."""
        return input(message)

    def handle_response(self, *, response: str):
        """Handle response from get_input."""
        if not response.lower() == "yes":
            raise exceptions.AbortBatchProcessError()


@dataclass
class LocalOutputHandler(OutputHandler):
    """Implements checks for an output directory on the local file system."""

    file_ids_with_extension: dict[str, str] = field(default_factory=dict, init=False)

    def check_output(self, *, location: Path) -> list[str]:
        """Check for and return existing files in output directory."""
        existing_files = []

        # check local files with and without extension
        for file_id, file_extension in self.file_ids_with_extension.items():
            if file_extension:
                file = location / f"{file_id}{file_extension}.c4gh"
            else:
                file = location / f"{file_id}.c4gh"

            if file.exists():
                existing_files.append(file_id)

        return existing_files


@dataclass
class CliIoHandler(BatchIoHandler):
    """Convenience class to hold both input and output handlers"""

    input_handler: CliInputHandler = field(default_factory=CliInputHandler, init=False)
    output_handler: LocalOutputHandler = field(
        default_factory=LocalOutputHandler, init=False
    )

    def check_output(self, *, location: Path) -> list[str]:
        """Check for and return existing files that would in output directory."""
        return self.output_handler.check_output(location=location)

    def get_input(self, *, message: str) -> str:
        """Simple user input handling."""
        return self.input_handler.get_input(message=message)

    def handle_response(self, *, response: str):
        """Handle response from get_input."""
        return self.input_handler.handle_response(response=response)


class FileStager:
    """Utility class to deal with file staging in batch processing."""

    staged: dict[str, URLResponse]  # successfully staged files with their urls
    unstaged: dict[str, float]  # files that are currently being staged with retry times
    failed: list[str]  # files that could not be staged

    message_display: AbstractMessageDisplay
    io_handler: BatchIoHandler
    output_dir: Path
    work_package_accessor: WorkPackageAccessor
    max_wait_time: int

    def __init__(  # noqa: PLR0913
        self,
        *,
        wanted_file_ids: list[str],
        dcs_api_url: str,
        output_dir: Path,
        max_wait_time: int,
        message_display: AbstractMessageDisplay,
        work_package_accessor: WorkPackageAccessor,
    ):
        """Initialize the FileStager."""
        io_handler = CliIoHandler()
        existing_file_ids = set(io_handler.check_output(location=output_dir))
        if not is_service_healthy(dcs_api_url):
            raise exceptions.ApiNotReachableError(api_url=dcs_api_url)
        self.api_url = dcs_api_url
        self.message_display = message_display
        self.work_package_accessor = work_package_accessor
        self.max_wait_time = max_wait_time
        self.time_started = now = time()
        self.staged = {}
        self.unstaged = {
            file_id: now
            for file_id in wanted_file_ids
            if file_id not in existing_file_ids
        }
        self.failed = []
        self.ignore_failed = False

    def get_staged_files(self):
        """Get files that are already staged."""
        staging_items = list(self.unstaged.items())
        for file_id, retry_time in staging_items:
            if time() >= retry_time:
                self._check_file(file_id=file_id)
        if not self.staged and not self._handle_failures():
            sleep(1)
        self._check_timeout()
        staged = self.staged.copy()
        self.staged.clear()
        return staged

    @property
    def finished(self) -> bool:
        """Check whether work is finished, i.e. no staged or unstaged files remain."""
        return not (self.staged or self.unstaged)

    def _check_file(self, file_id: str) -> None:
        """Check whether a file with the given file_id is staged."""
        try:
            with httpx_client() as client:
                url_and_headers = get_file_authorization(
                    file_id=file_id,
                    work_package_accessor=self.work_package_accessor,
                )
                response = get_download_url(
                    client=client, url_and_headers=url_and_headers
                )
        except exceptions.BadResponseCodeError as error:
            if error.response_code != 404:
                raise
            response = None
        if isinstance(response, URLResponse):
            del self.unstaged[file_id]
            self.staged[file_id] = response
        elif isinstance(response, RetryResponse):
            self.unstaged[file_id] = time() + response.retry_after
        else:
            self.failed.append(file_id)

    def _check_timeout(self):
        if time() - self.time_started >= self.max_wait_time:
            raise exceptions.MaxWaitTimeExceededError(max_wait_time=self.max_wait_time)

    def _handle_failures(self) -> bool:
        """Handle failed downloads and return whether there was user interaction."""
        if not self.failed or self.ignore_failed:
            return False
        failed = ", ".join(self.failed)
        message = f"No download exists for the following file IDs: {failed}"
        self.message_display.failure(message)
        if self.finished:
            raise exceptions.AbortBatchProcessError()
        unknown_ids_present = (
            "Some of the provided file IDs cannot be downloaded."
            + "\nDo you want to proceed ?\n[Yes][No]\n"
        )
        response = self.io_handler.get_input(message=unknown_ids_present)
        self.io_handler.handle_response(response=response)
        self.message_display.display("Downloading remaining files")
        self.time_started = time()  # reset the timer
        return True
