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
"""Module for btach processing specific code"""

import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import sleep

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls import check_url, get_download_url
from ghga_connector.core.message_display import AbstractMessageDisplay


@dataclass
class FileStager:  # pylint: disable=too-many-instance-attributes
    """Utility class to deal with file staging in batch processing."""

    api_url: str
    file_ids_with_extension: dict[str, str]
    message_display: AbstractMessageDisplay
    max_wait_time: int
    # amount of seconds between staging attempts
    retry_after: int = field(default=120)

    time_started: datetime = field(default_factory=datetime.utcnow, init=False)
    staged_files: list[str] = field(default_factory=list, init=False)
    unstaged_files: list[str] = field(default_factory=list, init=False)

    def check_and_stage(self, output_dir: Path):
        """Call DRS endpoint to stage files. Report file ids with 404 responses to user."""

        self._check_existing_files(output_dir=output_dir)

        # verify the API is actually reachable
        if not check_url(self.api_url):
            self.message_display.failure(
                f"The url {self.api_url} is currently not reachable."
            )
            raise exceptions.ApiNotReachableError(api_url=self.api_url)

        unknown_ids = []
        for file_id in self.file_ids_with_extension.keys():
            try:
                download_information = get_download_url(
                    api_url=self.api_url, file_id=file_id
                )
            except exceptions.BadResponseCodeError as error:
                if error.response_code == 404:
                    unknown_ids.append(file_id)
                    continue
                raise error

            # split into already staged and not yet staged files
            if download_information[0]:
                self.staged_files.append(file_id)
            else:
                self.unstaged_files.append(file_id)

        if unknown_ids:
            self._handle_unknown(unknown_ids)

    def update_staged_files(self):
        """
        Update staged file list after all previously staged files have been processed.
        Caller has to make sure, that all file ids in self.staged_files have actually
        been processed
        """
        self.staged_files = []
        remaining_unstaged = []

        for file_id in self.unstaged_files:
            dl_url = get_download_url(api_url=self.api_url, file_id=file_id)
            if dl_url[0]:
                self.staged_files.append(file_id)
            else:
                remaining_unstaged.append(file_id)

        self.unstaged_files = remaining_unstaged

        if self.unstaged_files and not self.staged_files:
            time_waited = datetime.utcnow() - self.time_started
            if time_waited.total_seconds() >= self.max_wait_time:
                raise exceptions.MaxWaitTimeExceededError(
                    max_wait_time=self.max_wait_time
                )
            self.message_display.display(
                f"No staged files available, retrying in {self.retry_after} seconds for "
                + f"{len(self.unstaged_files)} unstaged file(s)."
            )
            sleep(self.retry_after)

    def _check_existing_files(self, *, output_dir: Path):
        """Check if files would be overwritten and give user a choice if true"""
        existing_files = {}

        # check local files with and without extension
        for file_id, file_extension in self.file_ids_with_extension.items():
            if file_extension:
                file = output_dir / f"{file_id}.{file_extension}.c4gh"
            else:
                file = output_dir / f"{file_id}.c4gh"

            if file.exists():
                existing_files[file_id] = str(file)

        if existing_files:
            self._handle_existing_files(files=list(existing_files.values()))
            # remove existing file ids from those that should be staged
            for file_id in existing_files:
                del self.file_ids_with_extension[file_id]

    def _handle_existing_files(self, *, files: list[str]):
        """Prompt user interaction for already existing files"""

        existing_files = "\n -".join(files)
        message = f"The following files already exist locally:\n -{existing_files}"
        self.message_display.display(message)

        already_existing_files = (
            "Some files already exist locally and will be skipped."
            + "To redownload files, remove the already present file."
            + "Do you want to proceed with the remaining files?\n[Yes][No]\n"
        )
        response = _get_input(message=already_existing_files)
        self._handle_response(response=response)

    def _handle_response(self, *, response: str):
        """Handle user input"""
        if not response.lower() == "yes":
            self.message_display.display("Aborting batch process")
            sys.exit()

    def _handle_unknown(self, unknown_ids: list[str]):
        """Process user interaction for unknown file IDs"""
        message = (
            f"No download exists for the following file IDs: {' ,'.join(unknown_ids)}"
        )
        self.message_display.failure(message)

        unknown_ids_present = (
            "Some of the provided file IDs cannot be downloaded."
            + "\nDo you want to proceed ?\n[Yes][No]\n"
        )
        response = _get_input(message=unknown_ids_present)
        self._handle_response(response=response)
        self.message_display.display("Downloading remaining files")


def _get_input(message: str):
    """User input handling. Factored out to be patchable in tests."""
    return input(message)
