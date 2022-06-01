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

"""Custom Exceptions."""

from ghga_connector.core.constants import MAX_PART_NUMBER


class GHGAConnectorException(Exception):
    """
    Base Exception for all custom-thrown exceptions.
    Indicates expected behaviour such as user error or unstable connections
    """


class FatalError(Exception):
    """
    Base Exception for all exceptions that should not trigger retry logic
    """


class RetryTimeExpectedError(RuntimeError, GHGAConnectorException, FatalError):
    """Thrown, when a request didn't contain a retry time even though it was expected."""

    def __init__(self, url: str):
        message = (
            f"No `Retry-After` header in response from server following the url: {url}"
        )
        super().__init__(message)


class RequestFailedError(RuntimeError):
    """Thrown, when a request fails without returning a response code"""

    def __init__(self, url: str):
        message = f"The request to {url} failed."
        super().__init__(message)


class NoS3AccessMethod(RuntimeError, GHGAConnectorException, FatalError):
    """Thrown, when a request returns the desired response code, but no S3 Access Method"""

    def __init__(self, url: str):
        message = f"The request to {url} did not return an S3 Access Method."
        super().__init__(message)


class FileNotRegisteredError(RuntimeError, GHGAConnectorException, FatalError):
    """Thrown, when a request for a file returns a 404 error."""

    def __init__(self, file_id: str):
        message = (
            f"The request for the file {file_id} failed, "
            "because this file id does not exist."
        )
        super().__init__(message)


class UploadNotRegisteredError(RuntimeError, GHGAConnectorException, FatalError):
    """Thrown, when a request for a multipart upload returns a 404 error."""

    def __init__(self, upload_id: str):
        message = (
            f"The request for the upload with the id '{upload_id}' failed, "
            "because this upload does not exist."
        )
        super().__init__(message)


class BadResponseCodeError(RuntimeError, FatalError):
    """Thrown, when a request returns an unexpected response code (e.g. 500)"""

    def __init__(self, url: str, response_code: int):
        self.response_code = response_code
        message = f"The request to {url} failed with response code {response_code}"
        super().__init__(message)


class NoUploadPossibleError(RuntimeError, GHGAConnectorException, FatalError):
    """Thrown, when a multipart upload currently can't be started (response code 400)"""

    def __init__(self, file_id: str):
        message = (
            f"It is not possible to start a multipart upload for file with id '{file_id}', "
            "because this download is already pending or has been accepted."
        )
        super().__init__(message)


class UserHasNoUploadAccess(RuntimeError, GHGAConnectorException, FatalError):
    """
    Thrown when a user does not have the credentials to get or change
    details of an ongoing upload with a specific upload id
    (response code 403)
    """

    def __init__(self, upload_id: str):
        message = (
            "This user is not registered as data submitter "
            f"for the file corresponding to the upload_id '{upload_id}'."
        )
        super().__init__(message)


class UserHasNoFileAccess(RuntimeError, GHGAConnectorException, FatalError):
    """
    Thrown when a user does not have the credentials for
    a specific file id (response code 403)
    """

    def __init__(self, file_id: str):
        message = (
            "This user is not registered as data submitter "
            f"for the file with the id '{file_id}'."
        )
        super().__init__(message)


class CantChangeUploadStatus(RuntimeError, GHGAConnectorException, FatalError):
    """
    Thrown when the upload status of a file can't be set to the requested status
    (response code 400)
    """

    def __init__(self, upload_id: str, upload_status: str):
        message = f"The upload with id '{upload_id}' can't be set to '{upload_status}'."
        super().__init__(message)


class MaxWaitTimeExceeded(RuntimeError, GHGAConnectorException):
    """Thrown, when the specified wait time for getting a download url has been exceeded."""

    def __init__(self, max_wait_time: int):
        message = f"Exceeded maximum wait time of {max_wait_time} seconds."
        super().__init__(message)


class MaxRetriesReached(RuntimeError, GHGAConnectorException):
    """Thrown, when the specified number of retries has been exceeded."""

    def __init__(self, func_name: str, causes: list[Exception]):
        # keep track for testing purposes
        self.causes = causes
        message = (
            f"Exceeded maximum retries for '{func_name}'.\nExceptions encountered:\n"
        )
        for i, cause in enumerate(causes):
            message += f"{i+1}: {cause}\n"
        super().__init__(message)


class MaxPartNoExceededError(RuntimeError):
    """
    Thrown requesting a part number larger than the maximally possible number of parts.

    This exception is a bug.
    """

    def __init__(
        self,
    ):
        message = f"No more than ({MAX_PART_NUMBER}) file parts can be up-/downloaded."
        super().__init__(message)
