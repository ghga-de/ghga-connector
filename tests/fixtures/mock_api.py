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
Runs a small fastapi mock server for testing purposes.
All mocks work correclty with file_id == "1".
The drs3 mock sends back a "wait 1 minute" for file_id == "1m"
All other file_ids will fail
"""

from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from .models import AccessMethod, AccessURL, Checksum, DrsObjectServe, State
from .utils import UploadState

app = FastAPI()


@app.get("/objects/{file_id}", summary="drs3_mock")
def drs3_objects(file_id: str):

    """
    Mock for the drs3 /objects/{file_id} call
    """

    if file_id == "1m":
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED, content={"retry_after": 300}
        )

    if file_id == "1":

        download_url = "test"

        return DrsObjectServe(
            file_id=file_id,
            self_uri=f"drs://localhost:8080//{file_id}",
            size=1000,
            created_time=datetime.now(timezone.utc).isoformat(),
            updated_time=datetime.now(timezone.utc).isoformat(),
            checksums=[Checksum(checksum="1", type="md5")],
            access_methods=[
                AccessMethod(access_url=AccessURL(url=download_url), type="s3")
            ],
        )

    raise HTTPException(
        status_code=404,
        detail=('The DRSObject with the id "{file_id}" does not exist.'),
    )


@app.get(
    "/presigned_post/{file_id}", summary="ulc_presigned_post_mock", status_code=200
)
def ulc_presigned_post(file_id: str):

    """
    Mock for the ulc /presigned_post/{file_id} call.
    """

    upload_url = "test"

    if file_id == "1":
        return {"presigned_post": upload_url}

    raise HTTPException(
        status_code=404,
        detail=('The file with the file_id "{file_id}" does not exist.'),
    )


@app.patch(
    "/confirm_upload/{file_id}", summary="ulc_confirm_upload_mock", status_code=204
)
def ulc_confirm_upload(file_id: str, state: State):

    """
    Mock for the drs3 /confirm_upload/{file_id} call
    """

    if file_id == "1":
        if state.state == UploadState.REGISTERED:
            return status.HTTP_204_NO_CONTENT

        raise HTTPException(
            status_code=400,
            detail=(
                'The file with id "{file_id}" can`t be set to "{file_info_patch.state}"'
            ),
        )

    raise HTTPException(
        status_code=400,
        detail=(
            'The file with id "{file_id}" is registered for upload'
            + " but its content was not found in the inbox."
        ),
    )


def run_server():
    """
    Runs the fastapi instance
    """

    uvicorn.run(
        app,
        host="127.0.0.1",
        port="8080",
        log_level="info",
    )
