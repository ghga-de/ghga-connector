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
from typing import Generator

import pytest

from ghga_connector.core.retry import WithRetry


class RetryFixture:
    """A helper class to get and set (overwrite) ."""

    @property
    def max_retries(self) -> int:
        """returns the current max_retries value"""
        return WithRetry._max_retries  # type: ignore

    @max_retries.setter
    def max_retries(self, value: int):
        """Overwrite the default value of max_retries"""
        WithRetry._max_retries = value


@pytest.fixture
def retry_fixture() -> Generator[RetryFixture, None, None]:
    """
    Fixture dealing with cleanup for all tests touching functions
    annotated with the 'WithRetry' class decorator.
    Those tests need to request this fixture and use 'WithRetry.set_retries'
    with the yielded value as argument.
    As some tests call into functions that set 'WithRetry.maxretries'
    and it is not allowed to be set if it already has a non 'None' value,
    this is required for now
    """

    # set the max_retries default value for testing:
    WithRetry._max_retries = 0

    # provide functionality to overwrite the default
    yield RetryFixture()

    # Reset the max_retries parameter to None:
    WithRetry._max_retries = None
