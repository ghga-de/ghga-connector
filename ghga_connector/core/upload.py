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

import base64
import hashlib
import math
import os
from io import BufferedReader
from pathlib import Path

import crypt4gh.header
import crypt4gh.keys
import crypt4gh.lib
from nacl.bindings import crypto_aead_chacha20poly1305_ietf_encrypt

from ghga_connector.core import exceptions
from ghga_connector.core.api_calls import Uploader
from ghga_connector.core.client import async_client
from ghga_connector.core.file_operations import get_segments, read_file_parts


class Checksums:
    """Container for checksum calculation"""

    def __init__(self):
        self.unencrypted_sha256 = hashlib.sha256()
        self.encrypted_md5: list[str] = []
        self.encrypted_sha256: list[str] = []

    def __repr__(self) -> str:
        return (
            f"Unencrypted: {self.unencrypted_sha256.hexdigest()}\n"
            + f"Encrypted MD5: {self.encrypted_md5}\n"
            + f"Encrypted SHA256: {self.encrypted_sha256}"
        )

    def encrypted_is_empty(self):
        """TODO"""
        return len(self.encrypted_md5) > 0

    def get(self):
        """Return all checksums at the end of processing"""
        return (
            self.unencrypted_sha256.hexdigest(),
            self.encrypted_md5,
            self.encrypted_sha256,
        )

    def update_unencrypted(self, part: bytes):
        """Update checksum for unencrypted file"""
        self.unencrypted_sha256.update(part)

    def update_encrypted(self, part: bytes):
        """Update encrypted part checksums"""
        self.encrypted_md5.append(hashlib.md5(part, usedforsecurity=False).hexdigest())
        self.encrypted_sha256.append(hashlib.sha256(part).hexdigest())


class Encryptor:
    """Handles on the fly encryption and checksum calculation"""

    def __init__(
        self,
        part_size: int,
        private_key_path: Path,
        server_public_key: str,
        checksums: Checksums = Checksums(),
        file_secret: bytes = os.urandom(32),
    ):
        self.checksums = checksums
        self.encrypted_file_size = 0
        self.file_secret = file_secret
        self.part_size = part_size
        self.private_key_path = private_key_path
        self.server_public_key = base64.b64decode(server_public_key)

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
            segment, None, nonce, self.file_secret
        )  # no aad
        return nonce + encrypted_data

    def create_envelope(self) -> bytes:
        """
        Gather file encryption/decryption secret and assemble a crypt4gh envelope using the
        servers private and the clients public key
        """
        private_key = crypt4gh.keys.get_private_key(
            self.private_key_path, callback=None
        )
        keys = [(0, private_key, self.server_public_key)]
        header_content = crypt4gh.header.make_packet_data_enc(0, self.file_secret)
        header_packets = crypt4gh.header.encrypt(header_content, keys)
        header_bytes = crypt4gh.header.serialize(header_packets)

        return header_bytes

    # type annotation for file parts, should be generator
    def process_file(self, file: BufferedReader):
        """Encrypt and upload file parts."""
        unprocessed_bytes = b""
        upload_buffer = self.create_envelope()

        envelope_size = len(upload_buffer)

        for file_part in read_file_parts(file=file, part_size=self.part_size):
            # process unencrypted
            self.checksums.update_unencrypted(file_part)
            unprocessed_bytes += file_part

            # encrypt in chunks
            encrypted_bytes, unprocessed_bytes = self._encrypt(unprocessed_bytes)
            upload_buffer += encrypted_bytes

            # update checksums and yield if part size
            if len(upload_buffer) >= self.part_size:
                current_part = upload_buffer[: self.part_size]
                if self.checksums.encrypted_is_empty():
                    self.checksums.update_encrypted(current_part[envelope_size:])
                else:
                    self.checksums.update_encrypted(current_part)
                self.encrypted_file_size += self.part_size
                yield current_part
                upload_buffer = upload_buffer[self.part_size :]

        # process dangling bytes
        if unprocessed_bytes:
            upload_buffer += self._encrypt_segment(unprocessed_bytes)

        while len(upload_buffer) >= self.part_size:
            current_part = upload_buffer[: self.part_size]
            self.checksums.update_encrypted(current_part)
            self.encrypted_file_size += self.part_size
            yield current_part
            upload_buffer = upload_buffer[self.part_size :]

        if upload_buffer:
            self.checksums.update_encrypted(upload_buffer)
            self.encrypted_file_size += len(upload_buffer)
            yield upload_buffer

        self.encrypted_file_size -= envelope_size


class ChunkedUploader:
    """Handler class dealing with upload functionality"""

    def __init__(
        self, encryptor: Encryptor, file_path: Path, uploader: Uploader
    ) -> None:
        self.encrypted_file_size = 0
        self.encryptor = encryptor
        self.file_id = uploader.file_id
        self.input_path = file_path
        self.part_size = uploader.part_size
        self.unencrypted_file_size = file_path.stat().st_size
        self.uploader = uploader

    async def encrypt_and_upload(self):
        """Delegate encryption and perform multipart upload"""

        # compute encrypted_file_size
        num_segments = math.ceil(self.unencrypted_file_size / crypt4gh.lib.SEGMENT_SIZE)
        expected_encrypted_size = self.unencrypted_file_size + num_segments * 28

        with self.input_path.open("rb") as file:
            for part_number, part in enumerate(
                self.encryptor.process_file(file=file), start=1
            ):
                upload_url = await self.uploader.get_part_upload_url(
                    part_no=part_number
                )
                await self.uploader.upload_file_part(
                    presigned_url=upload_url, part=part
                )
            if expected_encrypted_size != self.encryptor.encrypted_file_size:
                raise exceptions.EncryptedSizeMismatch(
                    actual_encrypted_size=self.encryptor.encrypted_file_size,
                    expected_encrypted_size=expected_encrypted_size,
                )


async def run_upload(
    api_url: str,
    file_id: str,
    file_path: Path,
    private_key_path: Path,
    public_key_path: Path,
    server_public_key: str,
):
    """TODO"""

    async with async_client() as client:
        async with Uploader(
            api_url=api_url,
            client=client,
            file_id=file_id,
            public_key_path=public_key_path,
        ) as upload:
            await process_upload(
                uploader=upload,
                file_path=file_path,
                private_key_path=private_key_path,
                server_public_key=server_public_key,
            )


async def process_upload(
    uploader: Uploader, file_path: Path, private_key_path: Path, server_public_key: str
):
    """TODO"""
    encryptor = Encryptor(
        part_size=uploader.part_size,
        private_key_path=private_key_path,
        server_public_key=server_public_key,
    )
    chunked_uploader = ChunkedUploader(
        encryptor=encryptor, file_path=file_path, uploader=uploader
    )
    await chunked_uploader.encrypt_and_upload()
