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

"""
Runs a small fastapi mock server for testing purposes.
All mocks work correclty with file_id == "1".
The drs3 mock sends back a "wait 1 minute" for file_id == "1m"
All other file_ids will fail
"""

import base64
import json
import re
from datetime import datetime, timezone
from enum import Enum
from typing import List

try:  # workaround for https://github.com/pydantic/pydantic/issues/5821
    from typing_extensions import Literal
except ImportError:
    from typing import Literal  # type: ignore

import logging
from string import Template

import httpx
from fastapi import Header, HTTPException, Request, status
from fastapi.responses import Response
from pydantic import BaseModel

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class UploadStatus(str, Enum):
    """
    Enum for the possible UploadStatus of a specific upload_id
    """

    ACCEPTED = "accepted"
    CANCELLED = "cancelled"
    FAILED = "failed"
    PENDING = "pending"
    REJECTED = "rejected"
    UPLOADED = "uploaded"


class StatePatch(BaseModel):
    """
    Model containing a state parameter. Needed for the UCS patch: /uploads/... api call
    """

    status: UploadStatus


class StatePost(BaseModel):
    """
    Model containing a state parameter. Needed for the UCS post: /uploads api call
    """

    file_id: str


class PresignedPostURL(BaseModel):
    """
    Model containing an url
    """

    url: str


class Checksum(BaseModel):
    """
    A Checksum as per the DRS OpenApi specs.
    """

    checksum: str
    type: Literal["md5", "sha-256"]


class AccessURL(BaseModel):
    """Describes the URL for accessing the actual bytes of the object as per the
    DRS OpenApi spec."""

    url: str


class AccessMethod(BaseModel):
    """An AccessMethod as per the DRS OpenApi spec."""

    access_url: AccessURL
    type: Literal["s3"] = "s3"  # currently only s3 is supported


class UploadProperties(BaseModel):
    """The Upload Properties returned by the UCS post /uploads endpoint"""

    upload_id: str
    file_id: str
    part_size: int


class FileProperties(BaseModel):
    """The File Properties returned by the UCS get /files/{file_id} endpoint"""

    file_id: str
    file_name: str
    md5_checksum: str
    size: int
    grouping_label: str
    creation_date: datetime
    update_date: datetime
    format: str
    current_upload_id: str


class DrsObjectServe(BaseModel):
    """
    A model containing a DrsObject as per the DRS OpenApi specs.
    This is used to serve metadata on a DrsObject (including the access methods) to the
    user.
    """

    file_id: str  # the file ID
    self_uri: str
    size: int
    created_time: str
    updated_time: str
    checksums: List[Checksum]
    access_methods: List[AccessMethod]


class HttpEnvelopeResponse(httpx.Response):
    """Return base64 encoded envelope bytes"""

    response_id = "envelope"

    def __init__(self, *, envelope: str, status_code: int = 200):
        """Construct message and init the response."""

        super().__init__(content=envelope, status_code=status_code)


class HttpyException(Exception):
    """Testing stand in for httpyexpect HttpException without content validation"""

    def __init__(
        self, *, status_code: int, exception_id: str, description: str, data: dict
    ):
        self.status_code = status_code
        self.exception_id = exception_id
        self.description = description
        self.data = data
        super().__init__(description)


# app = FastAPI()


# @app.exception_handler(HttpyException)
def httpy_exception_handler(request: Request, exc: HttpyException):
    """Transform HttpException data into a proper response object"""

    return httpx.Response(
        status_code=exc.status_code,
        content=json.dumps(
            {
                "exception_id": exc.exception_id,
                "description": exc.description,
                "data": exc.data,
            }
        ).encode("utf-8"),
    )


# @app.get("/ready", summary="readiness_probe")
def ready():
    """
    Readyness probe.
    """
    return httpx.Response(status_code=status.HTTP_204_NO_CONTENT)


# @app.get("/objects/{file_id}", summary="drs3_mock")
def drs3_objects(file_id: str, authorization=Header()):
    """
    Mock for the drs3 /objects/{file_id} call
    """

    # simulate token authorization error
    if authorization == "Bearer authfail_normal":
        raise HTTPException(
            status_code=403, detail="This is not the token you're looking for."
        )

    # simulate token file_id/object_id mismatch
    if authorization == "Bearer file_id_mismatch":
        raise HttpyException(
            status_code=403,
            exception_id="wrongFileAuthorizationError",
            description="Endpoint file ID did not match file ID announced in work order token.",
            data={},
        )

    if file_id == "retry":
        return Response(
            status_code=status.HTTP_202_ACCEPTED, headers={"Retry-After": "10"}
        )

    if file_id in ("downloadable", "big-downloadable", "envelope-missing"):
        return httpx.Response(
            status_code=200,
            content=DrsObjectServe(
                file_id=file_id,
                self_uri=f"drs://localhost:8080//{file_id}",
                size=EnvironmentVars.S3_DOWNLOAD_FILE_SIZE,
                created_time=datetime.now(timezone.utc).isoformat(),
                updated_time=datetime.now(timezone.utc).isoformat(),
                checksums=[Checksum(checksum="1", type="md5")],
                access_methods=[
                    AccessMethod(
                        access_url=AccessURL(url=EnvironmentVars.S3_DOWNLOAD_URL),
                        type="s3",
                    )
                ],
            ).json(),
        )

    raise HTTPException(
        status_code=404,
        detail=(f'The DRSObject with the id "{file_id}" does not exist.'),
    )


# @app.get("/objects/{file_id}/envelopes/{public_key}", summary="drs3_envelope_mock")
def drs3_objects_envelopes(file_id: str, public_key: str, authorization=Header()):
    """
    Mock for the dcs /objects/{file_id}/envelopes/{public_key} call
    """

    if file_id in ("downloadable", "big-downloadable"):
        response_str = str.encode(EnvironmentVars.FAKE_ENVELOPE)
        envelope = base64.b64encode(response_str).decode("utf-8")
        return HttpEnvelopeResponse(envelope=envelope)

    raise HttpyException(
        status_code=404,
        exception_id="noSuchObject",
        description=(f'The DRSObject with the id "{file_id}" does not exist.'),
        data={"file_id": file_id},
    )


# @app.get("/files/{file_id}", summary="ulc_get_files_mock", status_code=200)
def ulc_get_files(file_id: str):
    """
    Mock for the ulc GET /files/{file_id} call.
    """

    if file_id == "pending":
        return FileProperties(
            file_id=file_id,
            file_name=file_id,
            md5_checksum="",
            size=0,
            grouping_label="inbox",
            creation_date=datetime.utcnow(),
            update_date=datetime.utcnow(),
            format="",
            current_upload_id="pending",
        )

    raise HttpyException(
        status_code=404,
        exception_id="fileNotRegistered",
        description=f'The file with the file_id "{file_id}" does not exist.',
        data={"file_id": file_id},
    )


# @app.get("/uploads/{upload_id}", summary="ulc_get_uploads_mock", status_code=200)
def ulc_get_uploads(upload_id: str):
    """
    Mock for the ulc GET /uploads/{upload_id} call.
    """
    if upload_id == "pending":
        return httpx.Response(
            status_code=200,
            content=UploadProperties(
                upload_id="pending",
                file_id="pending",
                part_size=EnvironmentVars.DEFAULT_PART_SIZE,
            ).json(),
        )

    raise HttpyException(
        status_code=404,
        exception_id="noSuchUpload",
        description=f'The upload with the id "{upload_id}" does not exist.',
        data={"upload_id": upload_id},
    )


# @app.post("/uploads", summary="ulc_post_uploads_mock", status_code=200)
def ulc_post_files_uploads(request: httpx.Request):
    """
    Mock for the ulc POST /uploads call.
    """
    content = json.loads(request.content)
    state: StatePost = StatePost(**content)

    file_id = state.file_id

    if file_id == "uploadable":
        return httpx.Response(
            status_code=200,
            content=UploadProperties(
                upload_id="pending",
                file_id=file_id,
                part_size=EnvironmentVars.DEFAULT_PART_SIZE,
            ).json(),
        )
    if file_id == "uploadable-16":
        return httpx.Response(
            status_code=200,
            content=UploadProperties(
                upload_id="pending",
                file_id=file_id,
                part_size=16 * 1024 * 1024,
            ).json(),
        )

    if file_id == "uploadable-8":
        return httpx.Response(
            status_code=200,
            content=UploadProperties(
                upload_id="pending",
                file_id=file_id,
                part_size=8 * 1024 * 1024,
            ).json(),
        )
    if file_id == "pending":
        raise HttpyException(
            status_code=403,
            exception_id="noFileAccess",
            description=f'Can`t start multipart upload for file with file id "{file_id}".',
            data={"file_id": file_id},
        )

    raise HttpyException(
        status_code=400,
        exception_id="fileNotRegistered",
        description=f'The file with the file_id "{file_id}" does not exist.',
        data={"file_id": file_id},
    )


# @app.post(
#     "/uploads/{upload_id}/parts/{part_no}/signed_urls",
#     summary="ulc_post_uploads_parts_files_signed_urls_mock",
#     status_code=200,
# )
def ulc_post_uploads_parts_files_signed_posts(
    upload_id: str, part_no: int, request: httpx.Request
):
    """
    Mock for the ulc POST /uploads/{upload_id}/parts/{part_no}/signed_urls call.
    """

    if upload_id == "pending":
        if part_no == 1:
            url = EnvironmentVars.S3_UPLOAD_URL_1
            return {"url": url}
        if part_no == 2:
            url = EnvironmentVars.S3_UPLOAD_URL_2
            return {"url": url}

    raise HttpyException(
        status_code=404,
        exception_id="noSuchUpload",
        description=f'The file with the upload id "{upload_id}" does not exist.',
        data={"upload_id": upload_id},
    )


# @app.patch("/uploads/{upload_id}", summary="ulc_patch_uploads_mock", status_code=204)
def ulc_patch_uploads(upload_id: str, request: httpx.Request):
    """
    Mock for the ulc PATCH /uploads/{upload_id} call
    """
    content = json.loads(request.content)
    state: StatePatch = StatePatch(**content)
    upload_status = state.status

    if upload_id == "uploaded":
        if upload_status == UploadStatus.CANCELLED:
            return httpx.Response(status_code=status.HTTP_204_NO_CONTENT)

        raise HttpyException(
            status_code=400,
            exception_id="uploadNotPending",
            description=f'The upload with id "{upload_id}" can`t be set to "{upload_status}"',
            data={"upload_id": upload_id, "current_upload_status": upload_id},
        )

    if upload_id == "pending":
        if upload_status == UploadStatus.UPLOADED:
            return httpx.Response(status_code=status.HTTP_204_NO_CONTENT)

        raise HttpyException(
            status_code=400,
            exception_id="uploadStatusChange",
            description=f'The upload with id "{upload_id}" can`t be set to "{upload_status}"',
            data={"upload_id": upload_id, "target_status": upload_status},
        )

    if upload_id == "uploadable":
        raise HttpyException(
            status_code=400,
            exception_id="uploadNotPending",
            description=f'The upload with id "{upload_id}" can`t be set to "{upload_status}"',
            data={"upload_id": upload_id, "current_upload_status": upload_id},
        )

    raise HttpyException(
        status_code=404,
        exception_id="noSuchUpload",
        description=f'The upload with id "{upload_id}" does not exist',
        data={"upload_id": upload_id},
    )


def create_work_order_token(
    package_id: str, file_id: str, request: httpx.Request, authorization=Header()
):
    """Mock Work Order Token endpoint"""

    # has to be at least 48 chars long
    return httpx.Response(
        status_code=201, content=base64.b64encode(b"1234567890" * 5).decode()
    )


def compile_regex_url(url_pattern: str, group_names: list[str]):
    """Given a url pattern, compile a regex that matches named groups where specified"""
    # e.g. "/work-packages/$package_id"
    # {"package_id": "package_id"}
    pattern = url_pattern.replace("/", "\\/")
    template = Template(pattern)
    mapping = {name: rf"(?P<{name}>[^\/]+)" for name in group_names}
    url = template.substitute(mapping)
    return url


get_map = {
    compile_regex_url("/uploads/$upload_id", ["upload_id"]): ulc_get_uploads,
    compile_regex_url("/files/$file_id", ["file_id"]): ulc_get_files,
    compile_regex_url(
        "/objects/$file_id/envelopes/$public_key", ["file_id", "public_key"]
    ): drs3_objects_envelopes,
    compile_regex_url("/objects/$file_id", ["file_id"]): drs3_objects,
    r"\/ready": ready,
    r"\/": ready,
}

post_map = {
    compile_regex_url(
        "/work-packages/$package_id/files/$file_id/work-order-tokens",
        ["package_id", "file_id"],
    ): create_work_order_token,
    compile_regex_url(
        "/uploads/$upload_id/parts/$part_no/signed_urls", ["upload_id", "part_no"]
    ): ulc_post_uploads_parts_files_signed_posts,
    r"\/uploads": ulc_post_files_uploads,
}

patch_map = {
    compile_regex_url("/uploads/$upload_id", ["upload_id"]): ulc_patch_uploads,
}

methods: dict[str, dict] = {"GET": get_map, "POST": post_map, "PATCH": patch_map}


class EnvironmentVars:
    """Mimic environment variables"""

    DEFAULT_PART_SIZE: int = 16 * 1024 * 1024
    S3_DOWNLOAD_URL: str = "test://download.url"
    S3_UPLOAD_URL_1: str = "test://upload.url"
    S3_UPLOAD_URL_2: str = "test://upload.url"
    S3_DOWNLOAD_FILE_SIZE: int = 146
    FAKE_ENVELOPE: str = "Fake_envelope"

    @classmethod
    def reset(cls):
        cls.DEFAULT_PART_SIZE = 16 * 1024 * 1024
        cls.S3_DOWNLOAD_URL = "test://download.url"
        cls.S3_UPLOAD_URL_1 = "test://upload.url"
        cls.S3_UPLOAD_URL_2 = "test://upload.url"
        cls.S3_DOWNLOAD_FILE_SIZE = 146
        cls.FAKE_ENVELOPE = "Fake_envelope"


def handle_request(request: httpx.Request):
    """Parse and pass request to appropriate function"""
    url = str(request.url)
    logger.info("Received request for url: %s", url)
    try:
        for regex, func in methods[request.method].items():
            trial = re.search(regex, url)
            if trial:
                logger.info("Going to call function: %s", func.__name__)
                params = trial.groupdict()
                if request.method in ["POST", "PATCH"]:
                    params["request"] = request
                logger.info("\tParameters are: ")
                for key, value in params.items():
                    logger.info("\t\t%s: %s", key, value)
                response = func(**params)
                logger.info("\tResponse received")
                return response
        return httpx.Response(status_code=500)
    except HttpyException as exc:
        return httpy_exception_handler(request=request, exc=exc)
