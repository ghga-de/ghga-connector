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

"""Data structures for file upload"""

import logging
from functools import cached_property
from math import ceil
from pathlib import Path

import crypt4gh.lib
from pydantic import computed_field

log = logging.getLogger(__name__)

NONCE_LENGTH = 12
AUTH_TAG_LENGTH = 16
ENVELOPE_SIZE = 124  # for one recipient (i.e. GHGA)
MIN_PART_SIZE = 5 * 1024**2  # 5 MiB (S3 minimum for multipart parts)
MAX_PART_SIZE = 5 * 1024**3  # 5 GiB (S3 maximum for multipart parts)


class CoreFileInfo:
    """The basic info about a file"""

    alias: str
    path: Path
    decrypted_size: int

    def __init__(self, *, alias: str, path: Path, decrypted_size: int):
        """Initialize the file info"""
        self.alias = alias
        self.path = path
        self.decrypted_size = decrypted_size

    @computed_field  # type: ignore
    @cached_property
    def encrypted_size(self) -> int:
        """The expected size of the encrypted file content, INCLUDING envelope."""
        # The number of encrypted chunks produced during encryption depends on the file
        #  size. ChaCha20Poly1305 encrypts SEGMENT_SIZE bytes at a time
        chunks = self.decrypted_size // crypt4gh.lib.SEGMENT_SIZE

        # Each full-length encrypted chunk in the file is CIPHER_SEGMENT_SIZE bytes long
        # The difference is 28 bytes. This comes from a 12-byte NONCE and a 16-byte auth tag.
        chunk_size = crypt4gh.lib.CIPHER_SEGMENT_SIZE

        # The last bytes of the file, which are less than SEGMENT_SIZE, are encrypted as
        #  is - no magic padding. So if there are 40 straggler bytes, it's 68 when encrypted.
        unencrypted_remainder = self.decrypted_size - crypt4gh.lib.SEGMENT_SIZE * chunks
        size_sans_envelope = chunk_size * chunks
        if unencrypted_remainder:
            size_sans_envelope += unencrypted_remainder + NONCE_LENGTH + AUTH_TAG_LENGTH
        return size_sans_envelope + ENVELOPE_SIZE


class FileInfoForUpload(CoreFileInfo):
    """A class containing information required to upload a file"""

    # This is similar to the FileInfo defined in the download path, but cannot be
    #  consolidated yet due to some differences we have to resolve in the future

    alias: str
    path: Path
    decrypted_size: int
    encrypted_size: int
    configured_part_size: int

    def __init__(self, *, core_file_info: CoreFileInfo, configured_part_size: int):
        """Initialize the file info"""
        self.alias = core_file_info.alias
        self.path = core_file_info.path
        self.decrypted_size = core_file_info.decrypted_size
        self.configured_part_size = configured_part_size

    @computed_field  # type: ignore
    @cached_property
    def part_size(self) -> int:
        """Calculate the adjusted part size which is both evenly divisible by
        the cipher segment size and ensures the file will contain < 10000 parts (the S3
        limit for multipart uploads). This ensures we download complete segments that
        can be decrypted.
        """
        segments_per_part = ceil(
            self.configured_part_size / crypt4gh.lib.CIPHER_SEGMENT_SIZE
        )
        adjusted_part_size = segments_per_part * crypt4gh.lib.CIPHER_SEGMENT_SIZE

        # If the adjusted size would result in hitting 10k parts, we need to increase
        #  the part size to bring that below the 10k threshold (we'll shoot for 9995)
        if ceil(self.encrypted_size / adjusted_part_size) >= 10_000:
            # Get a part size that breaks the encrypted content into 9995 parts, then
            #  divide evenly by CIPHER_SEGMENT_SIZE
            new_part_size = ceil((self.encrypted_size - ENVELOPE_SIZE) / 9_995)

            # Bring into alignment again, rounding part size up since we're trying to
            #  keep total part count down and it's already near the limit
            segments_per_part = ceil(new_part_size / crypt4gh.lib.CIPHER_SEGMENT_SIZE)
            adjusted_part_size = segments_per_part * crypt4gh.lib.CIPHER_SEGMENT_SIZE

        min_aligned = (
            ceil(MIN_PART_SIZE / crypt4gh.lib.CIPHER_SEGMENT_SIZE)
            * crypt4gh.lib.CIPHER_SEGMENT_SIZE
        )
        max_aligned = (
            MAX_PART_SIZE // crypt4gh.lib.CIPHER_SEGMENT_SIZE
        ) * crypt4gh.lib.CIPHER_SEGMENT_SIZE
        adjusted_part_size = max(min_aligned, min(max_aligned, adjusted_part_size))

        if adjusted_part_size != self.configured_part_size:
            log.info(
                "Adjusted part size from %d to %d bytes to align with Crypt4GH segment"
                + " boundaries for file %s.",
                self.configured_part_size,
                adjusted_part_size,
                self.alias,
            )
        return adjusted_part_size

    @computed_field  # type: ignore
    @cached_property
    def part_count(self) -> int:
        """Calculate the number of file parts"""
        return ceil(self.encrypted_size / self.part_size)
