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

"""Tests for the core functions of the cli"""

from contextlib import nullcontext
import pytest

from ghga_connector.core import check_url
from ghga_connector.core.decorators import Retry
from ghga_connector.core.exceptions import (
    FatalError,
    MaxRetriesReached,
    RequestFailedError,
)


@pytest.mark.parametrize(
    "api_url,wait_time,expected_response",
    # Google has a higher availability than ghga.de
    [("https://www.google.de/", 1000, True), ("https://bad_url", 1000, False)],
)
def test_check_url(api_url, wait_time, expected_response):
    """
    Test the check_url function
    """
    response = check_url(api_url, wait_time)
    assert response == expected_response


@pytest.mark.parametrize(
    "retry_exceptions,final_exception",
    [
        (
            [RuntimeError, TypeError, ValueError],
            MaxRetriesReached,
        )
        # (3, MaxRetriesReached, False),
        # (3, FatalError, True),
    ],
)
def test_retry(retry_exceptions: list[Exception], final_exception: Exception):
    """
    Test the Retry class decorator
    """
    # initialize state for the decorator
    Retry.max_retries = len(retry_exceptions) - 1

    curr_retry = 0

    @Retry
    def exception_producer() -> None:
        """
        Generate exceptions based on expected behavior
        Distinguish between fatal and non fatal exceptions
        """
        exception = retry_exceptions[curr_retry]
        curr_retry += 1

        if isinstance(exception, Exception):
            raise exception()

    try:
        exception_producer()
    except final_exception as final_error:
        if isinstance(final_error, MaxRetriesReached):
            for idx, retry_error in enumerate(final_error.causes):
                assert isinstance(retry_error, retry_exceptions[idx])

    # except Exception as exception:
    #     assert isinstance(exception, expected_exception)
    #     # Sanity check for number of retries
    #     if isinstance(exception, MaxRetriesReached):
    #         assert exception.num_causes == Retry.max_retries + 1
    # else:
    #     # this should be unreachable, except for exceptions not derived from Exception
    #     assert expected_exception is None
