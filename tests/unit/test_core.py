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
    "num_retries,expected_exception,is_fatal",
    [
        (0, None, False),
        (0, FatalError, False),
        (3, MaxRetriesReached, False),
        (3, FatalError, True),
    ],
)
def test_retry(num_retries, expected_exception, is_fatal):
    """
    Test the Retry class decorator
    """
    # initialize state for the decorator
    Retry.num_retries = num_retries

    @Retry
    def exception_producer(exception) -> None:
        """
        Generate exceptions based on expected behavior
        Distinguish between fatal and non fatal exceptions
        """
        # non fatal errors should trigger retry
        if isinstance(exception, MaxRetriesReached):
            raise RequestFailedError(
                "Retry. This should throw MaxRetriesReached inside the decorator"
            )
        # fatal errors should not trigger retry
        if is_fatal:
            raise FatalError("No Retry")
        # forgot to set fatal flag
        if isinstance(exception, FatalError):
            raise ValueError("Fatal Error needs 'is_fatal' set")
        # reraise unexpected exception to induce failure
        if exception is not None:
            raise exception(
                "Logic error in Decorator, this should be MaxRetriesReached"
            )

    try:
        exception_producer(expected_exception)
    except Exception as exception:
        assert isinstance(exception, expected_exception)
        # Sanity check for number of retries
        if isinstance(exception, MaxRetriesReached):
            assert exception.num_causes == Retry.num_retries + 1

    # this should be unreachable, except for exceptions not derived from Exception
    assert expected_exception is None
