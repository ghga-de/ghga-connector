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
#

"""Contains calls of the Presigned URLs in order to Up- and Download Files"""

import base64
import hashlib
import math
import os
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterator, Sequence
from io import BufferedReader
from pathlib import Path

import crypt4gh.header
import crypt4gh.keys
import crypt4gh.lib
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt

from ghga_connector.core.dataclasses import PartRange


class Checksums:
    """Container for checksum calculation"""

    def __init__(self):
        self._unencrypted_sha256 = hashlib.sha256()
        self._encrypted_md5: list[str] = []
        self._encrypted_sha256: list[str] = []

    def __repr__(self) -> str:
        return (
            f"Unencrypted: {self._unencrypted_sha256.hexdigest()}\n"
            + f"Encrypted MD5: {self._encrypted_md5}\n"
            + f"Encrypted SHA256: {self._encrypted_sha256}"
        )

    def encrypted_is_empty(self):
        """Returns true, the encryption checksum buffer is still empty"""
        return len(self._encrypted_md5) > 0

    def get(self):
        """Return all checksums at the end of processing"""
        return (
            self._unencrypted_sha256.hexdigest(),
            self._encrypted_md5,
            self._encrypted_sha256,
        )

    def update_unencrypted(self, part: bytes):
        """Update checksum for unencrypted file"""
        self._unencrypted_sha256.update(part)

    def update_encrypted(self, part: bytes):
        """Update encrypted part checksums"""
        self._encrypted_md5.append(hashlib.md5(part, usedforsecurity=False).hexdigest())
        self._encrypted_sha256.append(hashlib.sha256(part).hexdigest())


class Decryptor(ABC):
    """Convenience class to deal with file decryption"""

    @abstractmethod
    def decrypt_file(self, *, input_path: Path, output_path: Path):
        """Decrypt provided file"""


class Crypt4GHDecryptor(Decryptor):
    """Convenience class to deal with Crypt4GH decryption"""

    def __init__(self, decryption_key_path: Path):
        self._decryption_key = crypt4gh.keys.get_private_key(
            decryption_key_path, callback=None
        )

    def decrypt_file(self, *, input_path: Path, output_path: Path):
        """Decrypt provided file using Crypt4GH lib"""
        keys = [(0, self._decryption_key, None)]
        with input_path.open("rb") as infile, output_path.open("wb") as outfile:
            crypt4gh.lib.decrypt(keys=keys, infile=infile, outfile=outfile)


class Encryptor(ABC):
    """Handles on the fly encryption and checksum calculation"""

    @abstractmethod
    def get_encrypted_size(self) -> int:
        """Get file size after encryption, excluding envelope"""

    @abstractmethod
    def process_file(self, file: BufferedReader) -> Generator[bytes, None, None]:
        """Encrypt file parts and prepare for upload."""


class Crypt4GHEncryptor(Encryptor):
    """Handles on the fly encryption and checksum calculation"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        part_size: int,
        private_key_path: Path,
        server_public_key: str,
        checksums: Checksums = Checksums(),
        file_secret: bytes = os.urandom(32),
    ):
        self._encrypted_file_size = 0
        self._checksums = checksums
        self._file_secret = file_secret
        self._part_size = part_size
        self._private_key_path = private_key_path
        self._server_public_key = base64.b64decode(server_public_key)

    def _encrypt(self, part: bytes):
        """Encrypt file part using secret"""
        segments, incomplete_segment = get_segments(
            part=part, segment_size=crypt4gh.lib.SEGMENT_SIZE
        )

        encrypted_segments = []
        for segment in segments:
            encrypted_segments.append(self._encrypt_segment(segment))

        return b"".join(encrypted_segments), incomplete_segment

    def _encrypt_segment(self, segment: bytes):
        """Encrypt one single segment"""
        nonce = os.urandom(12)
        encrypted_data = crypto_aead_chacha20poly1305_ietf_encrypt(
            segment, None, nonce, self._file_secret
        )  # no aad
        return nonce + encrypted_data

    def _create_envelope(self) -> bytes:
        """
        Gather file encryption/decryption secret and assemble a crypt4gh envelope using the
        servers private and the clients public key
        """
        private_key = crypt4gh.keys.get_private_key(
            self._private_key_path, callback=None
        )
        keys = [(0, private_key, self._server_public_key)]
        header_content = crypt4gh.header.make_packet_data_enc(0, self._file_secret)
        header_packets = crypt4gh.header.encrypt(header_content, keys)
        header_bytes = crypt4gh.header.serialize(header_packets)

        return header_bytes

    def get_encrypted_size(self) -> int:
        """Get file size after encryption, excluding envelope"""
        return self._encrypted_file_size

    def process_file(self, file: BufferedReader) -> Generator[bytes, None, None]:
        """Encrypt file parts and prepare for upload."""
        unprocessed_bytes = b""
        upload_buffer = self._create_envelope()

        # get envelope size to adjust checksum buffers and encrypted content size
        envelope_size = len(upload_buffer)

        for file_part in read_file_parts(file=file, part_size=self._part_size):
            # process unencrypted
            self._checksums.update_unencrypted(file_part)
            unprocessed_bytes += file_part

            # encrypt in chunks
            encrypted_bytes, unprocessed_bytes = self._encrypt(unprocessed_bytes)
            upload_buffer += encrypted_bytes

            # update checksums and yield if part size
            if len(upload_buffer) >= self._part_size:
                current_part = upload_buffer[: self._part_size]
                if self._checksums.encrypted_is_empty():
                    self._checksums.update_encrypted(current_part[envelope_size:])
                else:
                    self._checksums.update_encrypted(current_part)
                self._encrypted_file_size += self._part_size
                yield current_part
                upload_buffer = upload_buffer[self._part_size :]

        # process dangling bytes
        if unprocessed_bytes:
            upload_buffer += self._encrypt_segment(unprocessed_bytes)

        while len(upload_buffer) >= self._part_size:
            current_part = upload_buffer[: self._part_size]
            self._checksums.update_encrypted(current_part)
            self._encrypted_file_size += self._part_size
            yield current_part
            upload_buffer = upload_buffer[self._part_size :]

        if upload_buffer:
            self._checksums.update_encrypted(upload_buffer)
            self._encrypted_file_size += len(upload_buffer)
            yield upload_buffer

        self._encrypted_file_size -= envelope_size


def is_file_encrypted(file_path: Path):
    """Checks if a file is Crypt4GH encrypted"""
    with file_path.open("rb") as input_file:
        num_relevant_bytes = 12
        file_header = input_file.read(num_relevant_bytes)

        magic_number = b"crypt4gh"
        version = b"\x01\x00\x00\x00"

        if file_header != magic_number + version:
            return False

    # If file header is correct, assume file is Crypt4GH encrypted
    return True


def calc_part_ranges(
    *, part_size: int, total_file_size: int, from_part: int = 1
) -> Sequence[PartRange]:
    """
    Calculate and return the ranges (start, end) of file parts as a list of tuples.

    By default it starts with the first part but you may also start from a specific part
    in the middle of the file using the `from_part` argument. This might be useful to
    resume an interrupted reading process.
    """
    # calc the ranges for the parts that have the full part_size:
    full_part_number = math.floor(total_file_size / part_size)
    part_ranges = [
        PartRange(start=part_size * (part_no - 1), stop=part_size * part_no - 1)
        for part_no in range(from_part, full_part_number + 1)
    ]

    if (total_file_size % part_size) > 0:
        # if the last part is smaller than the part_size, calculate its range separately:
        part_ranges.append(
            PartRange(start=part_size * full_part_number, stop=total_file_size - 1)
        )

    return part_ranges


def get_segments(part: bytes, segment_size: int):
    """Chunk file part into cipher segments"""
    full_segments = len(part) // segment_size
    segments = [
        part[i * segment_size : (i + 1) * segment_size] for i in range(full_segments)
    ]
    # get potential remainder of bytes that we need to handle
    # for non-matching boundaries between part and cipher segment size
    incomplete_segment = part[full_segments * segment_size :]
    return segments, incomplete_segment


def read_file_parts(
    file: BufferedReader, *, part_size: int, from_part: int = 1
) -> Iterator[bytes]:
    """
    Returns an iterator to iterate through file parts of the given size (in bytes).

    By default it start with the first part but you may also start from a specific part
    in the middle of the file using the `from_part` argument. This might be useful to
    resume an interrupted reading process.

    Please note: opening and closing of the file MUST happen outside of this function.
    """
    initial_offset = part_size * (from_part - 1)
    file.seek(initial_offset)

    while True:
        file_part = file.read(part_size)

        if len(file_part) == 0:
            return

        yield file_part
