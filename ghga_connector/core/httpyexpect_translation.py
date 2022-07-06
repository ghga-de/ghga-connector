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

"""
Adds wrapper classes to translate httpyexpect errors and check against
provided exception specs for all API endpoints
"""

from typing import Dict

import requests
from httpyexpect.client import ExceptionMapping, ResponseTranslator

from ghga_connector.core.exceptions import (
    CantChangeUploadStatus,
    FileNotRegisteredError,
    NoUploadPossibleError,
    UploadNotRegisteredError,
    UserHasNoFileAccess,
    UserHasNoUploadAccess,
)


class ResponseExceptionTranslator:
    """Base class providing behaviour and injection point for spec"""

    def __init__(self, spec: Dict[int, object]) -> None:
        self._exception_map = ExceptionMapping(spec)

    def handle(self, response: requests.Response):
        """Translate and raise error, if defined by spec"""
        translator = ResponseTranslator(response, exception_map=self._exception_map)
        translator.raise_for_error()


class FileMetadataTranslator(ResponseExceptionTranslator):
    """
    Handler for file metadata requests
    Endpoint: GET /files/{file_id}
    """

    def __init__(self, file_id: str) -> None:
        spec = {
            403: {"noFileAccess": lambda: UserHasNoFileAccess(file_id=file_id)},
            404: {"fileNotRegistered": lambda: FileNotRegisteredError(file_id=file_id)},
        }
        super().__init__(spec)


class UploadCreationTranslator(ResponseExceptionTranslator):
    """
    Handler for multipart upload initialization requests
    Endpoint: POST /uploads
    """

    def __init__(self, file_id: str) -> None:
        spec = {
            400: {
                "existingActiveUpload": lambda: NoUploadPossibleError(file_id=file_id),
                "fileNotRegistered": lambda: FileNotRegisteredError(file_id=file_id),
            },
            403: {"noFileAccess": lambda: UserHasNoFileAccess(file_id=file_id)},
        }
        super().__init__(spec)


class UploadDetailsTranslator(ResponseExceptionTranslator):
    """
    Handler for requests retrieving information about an ongoing upload
    Endpoint: GET /uploads/{upload_id}
    """

    def __init__(self, upload_id: str) -> None:
        spec = {
            403: {"noFileAccess": lambda: UserHasNoUploadAccess(upload_id=upload_id)},
            404: {
                "noSuchUpload": lambda: UploadNotRegisteredError(upload_id=upload_id)
            },
        }
        super().__init__(spec)


class PatchMultipartUploadTranslator(ResponseExceptionTranslator):
    """
    Handler for multipart upload state change requests
    Endpoint: PATCH /uploads/{upload_id}
    """

    def __init__(self, upload_id: str, upload_status: str) -> None:
        spec = {
            400: {
                "uploadNotPending": lambda: CantChangeUploadStatus(
                    upload_id=upload_id, upload_status=upload_status
                ),
                "uploadStatusChange": lambda: CantChangeUploadStatus(
                    upload_id=upload_id, upload_status=upload_status
                ),
            },
            403: {"noFileAccess": lambda: UserHasNoUploadAccess(upload_id=upload_id)},
            404: {
                "noSuchUpload": lambda: UploadNotRegisteredError(upload_id=upload_id)
            },
        }
        super().__init__(spec)


class PartUploadURLTranslator(ResponseExceptionTranslator):
    """
    Handler for signed part url retrieval requests
    Endpoint: /uploads/{upload_id}/parts/{part_no}/signed_urls
    """

    def __init__(self, upload_id: str) -> None:
        spec = {
            403: {"noFileAccess": lambda: UserHasNoUploadAccess(upload_id=upload_id)},
            404: {
                "noSuchUpload": lambda: UploadNotRegisteredError(upload_id=upload_id)
            },
        }
        super().__init__(spec)
