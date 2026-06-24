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

"""Unit tests for upload_files_from_list in batch_processing"""

from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from pydantic import SecretBytes

from ghga_connector import exceptions
from ghga_connector.core.uploading.batch_processing import (
    BatchPassResult,
    run_batch_upload,
    upload_files_from_list,
)
from ghga_connector.core.uploading.structs import UploadedFileInfo
from tests.fixtures.utils import (
    TEST_FILE_ID,
    TEST_STORAGE_ALIAS1,
    make_file_info_for_upload,
)

pytestmark = pytest.mark.asyncio


def make_mock_uploader(
    *,
    upload_raises: BaseException | None = None,
) -> AsyncMock:
    """Create a mock Uploader with configurable upload_file side effect."""
    uploader = AsyncMock()
    uploader.initiate_file_upload.return_value = TEST_FILE_ID, TEST_STORAGE_ALIAS1
    if upload_raises is not None:
        uploader.upload_file.side_effect = upload_raises
    return uploader


async def test_upload_files_from_list_success_does_not_call_delete():
    """Make sure delete_file is never called when an upload succeeds."""
    with NamedTemporaryFile() as f:
        file_info = make_file_info_for_upload(path=Path(f.name))
        mock_uploader = make_mock_uploader()

        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.Uploader",
                return_value=mock_uploader,
            ),
            patch("ghga_connector.core.uploading.batch_processing.Crypt4GHEncryptor"),
        ):
            await upload_files_from_list(
                upload_client=AsyncMock(),
                file_info_list=[file_info],
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
            )

        mock_uploader.upload_file.assert_called_once()
        mock_uploader.delete_file.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        exceptions.CreateFileUploadError(
            file_alias="test-file", exception=RuntimeError("something went wrong")
        ),
        KeyboardInterrupt(),
    ],
)
async def test_upload_files_from_list_deletes_on_errors(error: BaseException):
    """Make sure delete_file() is called when upload_file() raises
    either a KeyboardInterrupt or any other error.
    """
    with NamedTemporaryFile() as f:
        file_info = make_file_info_for_upload(path=Path(f.name))
        mock_uploader = make_mock_uploader(upload_raises=error)

        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.Uploader",
                return_value=mock_uploader,
            ),
            patch("ghga_connector.core.uploading.batch_processing.Crypt4GHEncryptor"),
        ):
            await upload_files_from_list(
                upload_client=AsyncMock(),
                file_info_list=[file_info],
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
            )

        mock_uploader.delete_file.assert_called_once()


async def test_upload_process_stops_on_failure():
    """Make sure that when initiate_file_upload() fails because the FileUploadBox lacks
    sufficient space, the batch stops, delete_file is NOT called (nothing to clean up),
    remaining files are skipped, and the function returns cleanly
    (no unhandled exception propagating to the caller).
    """
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        file_infos = [
            make_file_info_for_upload(path=Path(f1.name), alias="file-one"),
            make_file_info_for_upload(path=Path(f2.name), alias="file-two"),
        ]

        def _error():
            """Simulate error chain/re-raising of UploadBoxSizeExceededError"""
            try:
                raise exceptions.UploadBoxSizeExceededError(
                    file_alias="file-one", file_upload_box_id=uuid4()
                )
            except Exception as exc:
                raise exceptions.CreateFileUploadError(
                    file_alias="file-one", exception=exc
                ) from exc

        failing_uploader = AsyncMock()
        failing_uploader.initiate_file_upload.side_effect = _error
        second_uploader = make_mock_uploader()

        # Patch the Uploader and message display
        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.Uploader",
                side_effect=[failing_uploader, second_uploader],
            ),
            patch(
                "ghga_connector.core.uploading.batch_processing.CLIMessageDisplay"
            ) as mock_display,
        ):
            # Must return without raising
            await upload_files_from_list(
                upload_client=AsyncMock(),
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
            )

        failing_uploader.delete_file.assert_not_called()
        # Second file must not be started
        second_uploader.initiate_file_upload.assert_not_called()
        second_uploader.upload_file.assert_not_called()

        # Verify the user sees the error detail and the stop notification
        failure_messages = [
            call.args[0] for call in mock_display.failure.call_args_list
        ]
        assert any("file-one" in msg for msg in failure_messages)
        assert (
            "Upload process stopped."
            " If applicable, any previously completed file uploads remain uploaded."
        ) in failure_messages


async def test_upload_files_from_list_processes_each_file():
    """Make sure each file in the list has initiate_file_upload and upload_file called exactly once."""
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        file_infos = [
            make_file_info_for_upload(path=Path(f1.name), alias="file-one"),
            make_file_info_for_upload(path=Path(f2.name), alias="file-two"),
        ]
        mock_uploader_1 = make_mock_uploader()
        mock_uploader_2 = make_mock_uploader()

        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.Uploader",
                side_effect=[mock_uploader_1, mock_uploader_2],
            ),
            patch("ghga_connector.core.uploading.batch_processing.Crypt4GHEncryptor"),
        ):
            await upload_files_from_list(
                upload_client=AsyncMock(),
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
            )

        for mock_uploader in [mock_uploader_1, mock_uploader_2]:
            mock_uploader.initiate_file_upload.assert_called_once()
            mock_uploader.upload_file.assert_called_once()


async def test_upload_files_from_list_reports_failures():
    """Make sure that if a file upload results in an error, the file is returned in the
    result's failed list.
    """
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        file_infos = [
            make_file_info_for_upload(path=Path(f1.name), alias="ok"),
            make_file_info_for_upload(path=Path(f2.name), alias="bad"),
        ]
        good_uploader = make_mock_uploader()
        bad_uploader = make_mock_uploader(upload_raises=RuntimeError("problem"))

        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.Uploader",
                side_effect=[good_uploader, bad_uploader],
            ),
            patch("ghga_connector.core.uploading.batch_processing.Crypt4GHEncryptor"),
        ):
            result = await upload_files_from_list(
                upload_client=AsyncMock(),
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
            )

        assert not result.halted
        assert [fi.alias for fi in result.failed] == ["bad"]


async def test_upload_files_from_list_halts_on_box_full():
    """Test that when the box is full, processing stops and reports remaining files as failed."""
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        file_infos = [
            make_file_info_for_upload(path=Path(f1.name), alias="first"),
            make_file_info_for_upload(path=Path(f2.name), alias="second"),
        ]

        def _box_full():
            try:
                raise exceptions.UploadBoxSizeExceededError(
                    file_alias="first", file_upload_box_id=uuid4()
                )
            except Exception as exc:
                raise exceptions.CreateFileUploadError(
                    file_alias="first", exception=exc
                ) from exc

        failing_uploader = AsyncMock()
        failing_uploader.initiate_file_upload.side_effect = _box_full
        second_uploader = make_mock_uploader()

        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.Uploader",
                side_effect=[failing_uploader, second_uploader],
            ),
            patch("ghga_connector.core.uploading.batch_processing.Crypt4GHEncryptor"),
        ):
            result = await upload_files_from_list(
                upload_client=AsyncMock(),
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
            )

        assert result.halted
        # Both the file that triggered the error and the un-attempted file are reported.
        assert [fi.alias for fi in result.failed] == ["first", "second"]
        second_uploader.initiate_file_upload.assert_not_called()


def _make_upload_client(box_contents: list[UploadedFileInfo]) -> AsyncMock:
    """Create a mock UploadClient whose get_box_uploads returns box_contents."""
    upload_client = AsyncMock()
    upload_client.get_box_uploads.return_value = box_contents
    return upload_client


async def test_run_batch_upload_skips_already_uploaded():
    """Files already present (non-cancelled) in the box are not re-uploaded."""
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        file_infos = [
            make_file_info_for_upload(path=Path(f1.name), alias="already"),
            make_file_info_for_upload(path=Path(f2.name), alias="fresh"),
        ]
        upload_client = _make_upload_client(
            [UploadedFileInfo(id=uuid4(), alias="already", state="interrogated")]
        )

        captured: list[list[str]] = []

        async def fake_batch_cycle(*, file_info_list, **_kwargs):
            captured.append([fi.alias for fi in file_info_list])
            return BatchPassResult(failed=[], halted=False)

        with patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            fake_batch_cycle,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=0,
            )

        # Only the fresh file should have been passed to the uploader.
        assert captured == [["fresh"]]


async def test_run_batch_upload_does_not_skip_cancelled():
    """A cancelled (deleted) alias in the box is re-uploaded, not skipped."""
    with NamedTemporaryFile() as f:
        file_infos = [make_file_info_for_upload(path=Path(f.name), alias="redo")]
        upload_client = _make_upload_client(
            [UploadedFileInfo(id=uuid4(), alias="redo", state="cancelled")]
        )

        captured: list[list[str]] = []

        async def fake_batch_cycle(*, file_info_list, **_kwargs):
            captured.append([fi.alias for fi in file_info_list])
            return BatchPassResult(failed=[], halted=False)

        with patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            fake_batch_cycle,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=0,
            )

        assert captured == [["redo"]]


async def test_run_batch_upload_all_skipped_does_not_upload():
    """When every file is already uploaded, no upload pass is attempted."""
    with NamedTemporaryFile() as f:
        file_infos = [make_file_info_for_upload(path=Path(f.name), alias="done")]
        upload_client = _make_upload_client(
            [UploadedFileInfo(id=uuid4(), alias="done", state="interrogated")]
        )

        fake_batch_cycle = AsyncMock()
        with patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            fake_batch_cycle,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=3,
            )

        fake_batch_cycle.assert_not_called()


async def test_run_batch_upload_dry_run_does_not_upload():
    """In dry-run mode, already-uploaded files are still skipped but nothing uploads."""
    with NamedTemporaryFile() as f1, NamedTemporaryFile() as f2:
        file_infos = [
            make_file_info_for_upload(path=Path(f1.name), alias="already"),
            make_file_info_for_upload(path=Path(f2.name), alias="fresh"),
        ]
        upload_client = _make_upload_client(
            [UploadedFileInfo(id=uuid4(), alias="already", state="interrogated")]
        )

        fake_batch_cycle = AsyncMock()
        with (
            patch(
                "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
                fake_batch_cycle,
            ),
            patch(
                "ghga_connector.core.uploading.batch_processing.CLIMessageDisplay"
            ) as mock_display,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=file_infos,
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=3,
                dry_run=True,
            )

        # No upload pass is attempted in dry-run mode.
        fake_batch_cycle.assert_not_called()

        # The box is still queried so already-uploaded files are reported as skipped.
        upload_client.get_box_uploads.assert_awaited_once()
        messages = " ".join(
            str(call.args[0]) for call in mock_display.display.call_args_list
        )
        assert "Dry run" in messages
        # The fresh file is listed as one that would be uploaded; the skipped one is not.
        assert "fresh" in messages
        assert "Skipping 1 file(s)" in messages


async def test_run_batch_upload_dry_run_aligns_columns(tmp_path):
    """Dry-run file rows align their arrow, path and size columns vertically."""
    short = tmp_path / "a.bam"
    short.write_bytes(b"x")
    longer = tmp_path / "much-longer-name.bam"
    longer.write_bytes(b"x" * 2048)
    file_infos = [
        make_file_info_for_upload(path=short, alias="a", decrypted_size=1),
        make_file_info_for_upload(
            path=longer, alias="longer-alias", decrypted_size=2048
        ),
    ]

    with (
        patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            AsyncMock(),
        ),
        patch(
            "ghga_connector.core.uploading.batch_processing.CLIMessageDisplay"
        ) as mock_display,
    ):
        await run_batch_upload(
            upload_client=_make_upload_client([]),
            file_info_list=file_infos,
            my_private_key=SecretBytes(b"\x00" * 32),
            max_concurrent_uploads=1,
            dry_run=True,
        )

    all_lines = [str(call.args[0]) for call in mock_display.display.call_args_list]
    lines = [line for line in all_lines if line.startswith("  - ")]
    assert len(lines) == 2
    # The arrow and both parentheses occupy the same column in every row.
    assert len({line.index("->") for line in lines}) == 1
    assert len({line.index("(") for line in lines}) == 1
    assert len({line.index(")") for line in lines}) == 1

    # A header row labels the columns and lines up over them.
    header = next(line for line in all_lines if "ALIAS" in line)
    assert "PATH" in header and "SIZE" in header
    assert header.index("ALIAS") == len("  - ")
    assert header.index("PATH") == lines[0].index("->") + len("->  ")
    assert header.index("SIZE") + len("SIZE") == lines[0].index(")")


async def test_run_batch_upload_retries_failures():
    """Failed files are retried, and a retry that succeeds ends the loop."""
    with NamedTemporaryFile() as f:
        failing = make_file_info_for_upload(path=Path(f.name), alias="flaky")
        upload_client = _make_upload_client([])

        calls: list[list[str]] = []

        async def fake_batch_cycle(*, file_info_list, **_kwargs):
            calls.append([fi.alias for fi in file_info_list])
            # Fail on the first pass, succeed on the retry.
            if len(calls) == 1:
                return BatchPassResult(failed=[failing], halted=False)
            return BatchPassResult(failed=[], halted=False)

        with patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            fake_batch_cycle,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=[failing],
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=3,
            )

        assert calls == [["flaky"], ["flaky"]]


async def test_run_batch_upload_stops_after_max_retries():
    """Persistent failures stop after the initial pass plus max_retries retries."""
    with NamedTemporaryFile() as f:
        failing = make_file_info_for_upload(path=Path(f.name), alias="doomed")
        upload_client = _make_upload_client([])

        call_count = 0

        async def fake_batch_cycle(*, file_info_list, **_kwargs):
            nonlocal call_count
            call_count += 1
            return BatchPassResult(failed=[failing], halted=False)

        with patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            fake_batch_cycle,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=[failing],
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=2,
            )

        # 1 initial pass + 2 retries
        assert call_count == 3


async def test_run_batch_upload_does_not_retry_when_halted():
    """A halted pass (e.g. box full) is not retried even if retries remain."""
    with NamedTemporaryFile() as f:
        failing = make_file_info_for_upload(path=Path(f.name), alias="halt-me")
        upload_client = _make_upload_client([])

        call_count = 0

        async def fake_batch_cycle(*, file_info_list, **_kwargs):
            nonlocal call_count
            call_count += 1
            return BatchPassResult(failed=[failing], halted=True)

        with patch(
            "ghga_connector.core.uploading.batch_processing.upload_files_from_list",
            fake_batch_cycle,
        ):
            await run_batch_upload(
                upload_client=upload_client,
                file_info_list=[failing],
                my_private_key=SecretBytes(b"\x00" * 32),
                max_concurrent_uploads=1,
                max_retries=5,
            )

        assert call_count == 1
