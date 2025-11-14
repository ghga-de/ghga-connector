# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Functionality to encrypt files in chunks with Crypt4GH before upload."""

import base64
import math
import os
from collections.abc import Generator
from io import BufferedReader
from typing import Any

import crypt4gh.header
import crypt4gh.lib
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt
from pydantic import SecretBytes

from ghga_connector import exceptions
from ghga_connector.config import get_ghga_pubkey
from ghga_connector.core.crypt.checksums import Checksums
from ghga_connector.core.file_operations import get_segments, read_file_parts

FileProcessor = Generator[tuple[int, bytes], Any, None]


class Crypt4GHEncryptor:
    """Handles on the fly encryption and checksum calculation"""

    def __init__(self, part_size: int, my_private_key: SecretBytes, file_size: int):
        self._part_size = part_size
        self._my_private_key = my_private_key
        self._server_public_key = base64.b64decode(get_ghga_pubkey())
        self._file_secret = os.urandom(32)
        self.checksums = Checksums()  # Updated as encryption takes place
        self._encrypted_file_size = 0  # Updated as encryption takes place
        num_segments = math.ceil(file_size / crypt4gh.lib.SEGMENT_SIZE)
        self.expected_encrypted_size = file_size + num_segments * 28

    def _encrypt(self, part: bytes):
        """Encrypt file part using secret"""
        segments, incomplete_segment = get_segments(
            part=part, segment_size=crypt4gh.lib.SEGMENT_SIZE
        )
        encrypted_segments = [self._encrypt_segment(segment) for segment in segments]
        return b"".join(encrypted_segments), incomplete_segment

    def _encrypt_segment(self, segment: bytes) -> bytes:
        """Encrypt one single segment"""
        nonce = os.urandom(12)
        encrypted_data = crypto_aead_chacha20poly1305_ietf_encrypt(
            segment, None, nonce, self._file_secret
        )  # no aad
        return nonce + encrypted_data

    def _create_envelope(self) -> bytes:
        """
        Gather file encryption/decryption secret and assemble a crypt4gh envelope using the
        server's private and the client's public key
        """
        keys = [(0, self._my_private_key.get_secret_value(), self._server_public_key)]
        header_content = crypt4gh.header.make_packet_data_enc(0, self._file_secret)
        header_packets = crypt4gh.header.encrypt(header_content, keys)
        header_bytes = crypt4gh.header.serialize(header_packets)
        return header_bytes

    def get_encrypted_size(self) -> int:
        """Get file size after encryption, excluding envelope"""
        return self._encrypted_file_size

    def process_file(self, *, file: BufferedReader) -> FileProcessor:
        """Encrypt file parts for upload, yielding a tuple of the part number and content."""
        unprocessed_bytes = b""
        upload_buffer = self._create_envelope()

        # get envelope size to adjust checksum buffers and encrypted content size
        envelope_size = len(upload_buffer)

        for part_number, file_part in enumerate(
            read_file_parts(file=file, part_size=self._part_size), start=1
        ):
            # process unencrypted
            self.checksums.update_unencrypted(file_part)
            unprocessed_bytes += file_part

            # encrypt in chunks
            encrypted_bytes, unprocessed_bytes = self._encrypt(unprocessed_bytes)
            upload_buffer += encrypted_bytes

            # update checksums and yield if part size
            if len(upload_buffer) >= self._part_size:
                current_part = upload_buffer[: self._part_size]
                if self.checksums.encrypted_is_empty():
                    self.checksums.update_encrypted(current_part[envelope_size:])
                else:
                    self.checksums.update_encrypted(current_part)
                self._encrypted_file_size += self._part_size
                yield part_number, current_part
                upload_buffer = upload_buffer[self._part_size :]

        self._encrypted_file_size -= envelope_size

        # process dangling bytes
        if unprocessed_bytes:
            upload_buffer += self._encrypt_segment(unprocessed_bytes)

        while len(upload_buffer) >= self._part_size:
            current_part = upload_buffer[: self._part_size]
            self.checksums.update_encrypted(current_part)
            self._encrypted_file_size += self._part_size
            part_number += 1
            yield part_number, current_part
            upload_buffer = upload_buffer[self._part_size :]

        if upload_buffer:
            self.checksums.update_encrypted(upload_buffer)
            self._encrypted_file_size += len(upload_buffer)
            part_number += 1
            yield part_number, upload_buffer

        # Finally, verify the encrypted size
        if self.expected_encrypted_size != self._encrypted_file_size:
            raise exceptions.EncryptedSizeMismatch(
                actual_encrypted_size=self._encrypted_file_size,
                expected_encrypted_size=self.expected_encrypted_size,
            )
