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

"""Global Config Parameters"""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any

import httpx
from hexkit.config import config_from_yaml
from hexkit.utils import set_context_var
from pydantic import Field, NonNegativeInt, PositiveInt
from pydantic_settings import BaseSettings

from ghga_connector import exceptions
from ghga_connector.constants import DEFAULT_PART_SIZE, MAX_RETRIES, MAX_WAIT_TIME

__all__ = [
    "CONFIG",
    "Config",
    "get_dcs_api_url",
    "get_ghga_pubkey",
    "get_ucs_api_url",
    "get_wps_api_url",
    "set_runtime_config",
]

ucs_api_url_var: ContextVar[str] = ContextVar("ucs_api_url", default="")
dcs_api_url_var: ContextVar[str] = ContextVar("dcs_api_url", default="")
wps_api_url_var: ContextVar[str] = ContextVar("wps_api_url", default="")
ghga_pubkey_var: ContextVar[str] = ContextVar("ghga_pubkey", default="")


def _get_context_var(context_var: ContextVar) -> Any:
    value = context_var.get()
    if not value:
        raise ValueError(f"{context_var.name} is not set")
    return value


def get_ucs_api_url() -> str:
    """Get the UCS API URL."""
    return _get_context_var(ucs_api_url_var)


def get_dcs_api_url() -> str:
    """Get the DCS API URL."""
    return _get_context_var(dcs_api_url_var)


def get_wps_api_url() -> str:
    """Get the WPS API URL."""
    return _get_context_var(wps_api_url_var)


def get_ghga_pubkey() -> str:
    """Get the GHGA crypt4gh public key."""
    return _get_context_var(ghga_pubkey_var)


@config_from_yaml(prefix="ghga_connector")
class Config(BaseSettings):
    """Global Config Parameters"""

    max_concurrent_downloads: PositiveInt = Field(
        default=5, description="Number of parallel downloader tasks for file parts."
    )
    max_retries: NonNegativeInt = Field(
        default=MAX_RETRIES, description="Number of times to retry failed API calls."
    )
    max_wait_time: PositiveInt = Field(
        default=MAX_WAIT_TIME,
        description="Maximum time in seconds to wait before quitting without a download.",
    )
    part_size: PositiveInt = Field(
        default=DEFAULT_PART_SIZE, description="The part size to use for download."
    )
    wkvs_api_url: str = Field(
        default="https://data.ghga.de/.well-known",
        description="URL to the root of the WKVS API. Should start with https://",
    )
    exponential_backoff_max: NonNegativeInt = Field(
        default=60,
        description="Maximum number of seconds to wait for when using exponential backoff retry strategies.",
    )
    retry_status_codes: list[NonNegativeInt] = Field(
        default=[408, 500, 502, 503, 504],
        description="List of status codes that should trigger retrying a request.",
    )


CONFIG = Config()


@asynccontextmanager
async def set_runtime_config(client: httpx.AsyncClient):
    """Set runtime config as context vars to be accessed within a context manager.

    This sets the following values:
    - ghga_pubkey
    - wps_api_url
    - dcs_api_url
    - ucs_api_url
    """
    ghga_pubkey = await _get_wkvs_value(client, value_name="crypt4gh_public_key")
    wps_api_url = (await _get_wkvs_value(client, value_name="wps_api_url")).rstrip("/")
    dcs_api_url = (await _get_wkvs_value(client, value_name="dcs_api_url")).rstrip("/")
    ucs_api_url = (await _get_wkvs_value(client, value_name="ucs_api_url")).rstrip("/")

    async with (
        set_context_var(ghga_pubkey_var, ghga_pubkey),
        set_context_var(wps_api_url_var, wps_api_url),
        set_context_var(dcs_api_url_var, dcs_api_url),
        set_context_var(ucs_api_url_var, ucs_api_url),
    ):
        yield


async def _get_wkvs_value(client: httpx.AsyncClient, *, value_name: str) -> Any:
    """Retrieve a value from the well-known-value-service.

    Args:
        value_name (str): the name of the value to be retrieved

    Raises:
        WellKnownValueNotFound: when a 404 response is received from the WKVS
        KeyError: when a successful response is received but doesn't contain the expected value
    """
    url = f"{CONFIG.wkvs_api_url}/values/{value_name}"

    try:
        response = await client.get(url)  # verify is True by default
    except httpx.RequestError as request_error:
        exceptions.raise_if_connection_failed(request_error=request_error, url=url)
        raise exceptions.RequestFailedError(url=url) from request_error

    if response.status_code == 404:
        raise exceptions.WellKnownValueNotFound(value_name=value_name)

    try:
        value = response.json()[value_name]
    except KeyError as err:
        raise KeyError(
            "Response from well-known-value-service did not include expected field"
            + f" '{value_name}'"
        ) from err
    return value
