# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Handling session initialization for httpx"""

from contextlib import asynccontextmanager, contextmanager

import httpx
from tenacity import (
    AsyncRetrying,
    Retrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ghga_connector.core.constants import TIMEOUT


class HttpxClientConfigurator:
    """Helper class to make max_retries user configurable"""

    max_retries: int

    @classmethod
    def configure(cls, max_retries: int):
        """Configure client with exponential backoff retry (using httpx's 0.5 default)"""
        # can't be negative - should we log this?
        cls.max_retries = max(0, max_retries)


def configure_async_retries(
    exponential_backoff_max: int, max_retries: int, status_codes: list[int]
):
    """Initialize retry handler from config"""
    return AsyncRetrying(
        reraise=True,
        retry=(
            retry_if_exception_type(
                (
                    httpx.ConnectError,
                    httpx.ConnectTimeout,
                    httpx.TimeoutException,
                )
            )
            | retry_if_result(lambda response: response.status_code in status_codes)
        ),
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential_jitter(max=exponential_backoff_max),
    )


def configure_retries(
    exponential_backoff_max: int, max_retries: int, status_codes: list[int]
):
    """Initialize retry handler from config"""
    return Retrying(
        reraise=True,
        retry=(
            retry_if_exception_type(
                (
                    httpx.ConnectError,
                    httpx.ConnectTimeout,
                    httpx.TimeoutException,
                )
            )
            | retry_if_result(lambda response: response.status_code in status_codes)
        ),
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential_jitter(max=exponential_backoff_max),
    )


@contextmanager
def httpx_client():
    """Yields a context manager httpx client and closes it afterward"""
    with httpx.Client(timeout=TIMEOUT) as client:
        yield client


@asynccontextmanager
async def async_client():
    """Yields a context manager async httpx client and closes it afterward"""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        yield client
