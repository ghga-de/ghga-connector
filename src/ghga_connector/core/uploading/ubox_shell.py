# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""An interactive shell for managing the contents of a FileUploadBox.

The shell offers ``upload``, ``ls`` and ``rm`` commands that reuse a single
authenticated session, so the access token only has to be entered once.
"""

import glob
import logging
import shlex

from pydantic import SecretBytes

from ghga_connector.config import get_config
from ghga_connector.core.message_display import CLIMessageDisplay
from ghga_connector.core.uploading.api_calls import UploadClient
from ghga_connector.core.uploading.batch_processing import (
    parse_file_info_for_upload,
    upload_files_from_list,
)
from ghga_connector.core.uploading.structs import FileInfoForUpload, UploadedFileInfo

log = logging.getLogger(__name__)

HELP_TEXT = """Available commands:
  upload PATH [PATH ...]      Upload one or more files/globs, using each local
                              file name as its alias.
  upload --alias ALIAS PATH   Upload a single file under the given alias.
  ls                          List the contents of the upload box.
  rm ALIAS                    Delete the file with the given alias from the box.
  help                        Show this help text.
  exit | quit                 Leave the shell (Ctrl+D also works).
"""


def _human_readable_size(num_bytes: int | None) -> str:
    """Render a byte count in a compact, human-readable form."""
    if num_bytes is None:
        return "-"
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(size) < 1024 or unit == "PiB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PiB"


class UboxShell:
    """A small REPL for interacting with a single FileUploadBox."""

    PROMPT = "ubox> "

    def __init__(self, *, upload_client: UploadClient, my_private_key: SecretBytes):
        self._upload_client = upload_client
        self._my_private_key = my_private_key

    async def run(self) -> None:
        """Run the interactive read-eval-print loop until the user exits."""
        CLIMessageDisplay.display(
            "Entering upload box shell. Type 'help' for a list of commands and"
            + " 'exit' to leave."
        )
        while True:
            try:
                line = input(self.PROMPT)
            except EOFError:
                # Ctrl+D - print a newline so the next prompt isn't glued to it
                CLIMessageDisplay.display("")
                break
            except KeyboardInterrupt:
                # Ctrl+C on the prompt cancels the current line rather than exiting
                CLIMessageDisplay.display("")
                continue

            if not await self._handle_line(line):
                break

    async def _handle_line(self, line: str) -> bool:
        """Parse and execute a single input line.

        Returns False if the shell should exit, True if it should continue.
        """
        line = line.strip()
        if not line:
            return True

        try:
            tokens = shlex.split(line)
        except ValueError as err:
            CLIMessageDisplay.failure(f"Could not parse input: {err}")
            return True

        command, *args = tokens
        command = command.lower()

        if command in ("exit", "quit"):
            return False
        if command == "help":
            CLIMessageDisplay.display(HELP_TEXT)
            return True

        handler = {
            "upload": self._do_upload,
            "ls": self._do_ls,
            "rm": self._do_rm,
        }.get(command)

        if handler is None:
            CLIMessageDisplay.failure(
                f"Unknown command: '{command}'. Type 'help' for usage."
            )
            return True

        try:
            await handler(args)
        except KeyboardInterrupt:
            # Already handled within long-running operations (e.g. uploads);
            # keep the shell alive.
            pass
        except Exception as err:
            CLIMessageDisplay.failure(str(err))
        return True

    async def _do_upload(self, args: list[str]) -> None:
        """Handle the 'upload' command."""
        if not args:
            CLIMessageDisplay.failure("Usage: upload [--alias ALIAS] PATH [PATH ...]")
            return

        try:
            alias, path_tokens = _extract_alias(args)
        except ValueError as err:
            CLIMessageDisplay.failure(str(err))
            return

        if alias is not None:
            expanded = _expand_globs(path_tokens)
            if len(expanded) != 1:
                CLIMessageDisplay.failure(
                    "The --alias option requires exactly one file path."
                )
                return
            file_info_strings = [f"{alias},{expanded[0]}"]
        else:
            if not path_tokens:
                CLIMessageDisplay.failure("No file path supplied.")
                return
            file_info_strings = _expand_globs(path_tokens)

        try:
            core_file_info = parse_file_info_for_upload(file_info_strings)
        except Exception as err:
            CLIMessageDisplay.failure(str(err))
            return

        config = get_config()
        file_info_list = [
            FileInfoForUpload(core_file_info=cfi, configured_part_size=config.part_size)
            for cfi in core_file_info
        ]
        await upload_files_from_list(
            upload_client=self._upload_client,
            file_info_list=file_info_list,
            my_private_key=self._my_private_key,
            max_concurrent_uploads=config.max_concurrent_uploads,
        )

    async def _do_ls(self, args: list[str]) -> None:
        """Handle the 'ls' command."""
        if args:
            CLIMessageDisplay.failure("Usage: ls (takes no arguments)")
            return

        uploads = await self._upload_client.get_box_uploads()
        if not uploads:
            CLIMessageDisplay.display("The upload box is empty.")
            return

        CLIMessageDisplay.display(_format_listing(uploads))

    async def _do_rm(self, args: list[str]) -> None:
        """Handle the 'rm' command."""
        if len(args) != 1:
            CLIMessageDisplay.failure("Usage: rm ALIAS")
            return

        alias = args[0]
        uploads = await self._upload_client.get_box_uploads()
        match = next((upload for upload in uploads if upload.alias == alias), None)
        if match is None:
            CLIMessageDisplay.failure(
                f"No file with alias '{alias}' was found in the upload box."
            )
            return

        await self._upload_client.delete_file(file_id=match.file_id, file_alias=alias)
        CLIMessageDisplay.success(f"Deleted '{alias}' from the upload box.")


def _extract_alias(args: list[str]) -> tuple[str | None, list[str]]:
    """Split out an optional ``--alias`` value from the argument list.

    Returns a 2-tuple of the alias (or None) and the remaining positional tokens.

    Raises:
        ValueError: If --alias is given without a value or more than once.
    """
    alias: str | None = None
    remaining: list[str] = []
    i = 0
    while i < len(args):
        item = args[i]
        if item == "--alias":
            if alias is not None:
                raise ValueError("The --alias option may only be specified once.")
            if i + 1 >= len(args):
                raise ValueError("The --alias option requires a value.")
            alias = args[i + 1]
            i += 2
        elif item.startswith("--alias="):
            if alias is not None:
                raise ValueError("The --alias option may only be specified once.")
            alias = item.split("=", 1)[1]
            if not alias:
                raise ValueError("The --alias option requires a value.")
            i += 1
        else:
            remaining.append(item)
            i += 1
    return alias, remaining


def _expand_globs(tokens: list[str]) -> list[str]:
    """Expand any glob patterns in the supplied tokens.

    Tokens that do not match any file are passed through unchanged so that the
    downstream path validation can raise a clear "file does not exist" error.
    """
    expanded: list[str] = []
    for token in tokens:
        matches = sorted(glob.glob(token))
        if matches:
            expanded.extend(matches)
        else:
            expanded.append(token)
    return expanded


def _format_listing(uploads: list[UploadedFileInfo]) -> str:
    """Format the box contents as an aligned table."""
    headers = ("ALIAS", "SIZE", "STATE", "FILE ID")
    rows = [
        (
            upload.alias,
            _human_readable_size(upload.decrypted_size),
            upload.state or "-",
            str(upload.file_id),
        )
        for upload in uploads
    ]

    widths = [
        max(len(headers[col]), *(len(row[col]) for row in rows))
        for col in range(len(headers))
    ]

    def _format_row(row: tuple[str, ...]) -> str:
        return "  ".join(value.ljust(widths[col]) for col, value in enumerate(row))

    lines = [_format_row(headers)]
    lines.extend(_format_row(row) for row in rows)
    return "\n".join(lines)
