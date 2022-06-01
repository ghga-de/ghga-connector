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

"""Reusable decorators"""
import time
from typing import Any, Callable

from ghga_connector.core.constants import MAX_RETRIES
from ghga_connector.core.exceptions import FatalError, MaxRetriesReached


class Retry:
    """Class decorator providing common retry logic"""

    max_retries: int = MAX_RETRIES

    def __init__(self, func: Callable) -> None:
        """
        Class decorators get the decorated function as argument
        and need to store it for the actual call
        """
        self.func = func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Decorator needs to be callable.
        Provided args, kwargs are passed directly to the decorated function
        """

        def retry():
            exception_causes = []
            # try calling decorated function at least once
            for i in range(Retry.max_retries + 1):
                try:
                    func = self.func(*args, **kwargs)
                    return func
                except Exception as exception:  # pylint: disable=broad-except
                    if isinstance(exception, FatalError):
                        raise exception
                    exception_causes.append(exception)
                    # Use exponential backoff for retries
                    backoff_factor = 0.5
                    exponential_backoff = backoff_factor * (2 ** (i))
                    time.sleep(exponential_backoff)
            raise MaxRetriesReached(self.func.__name__, exception_causes)

        return retry()

    @classmethod
    def set_retries(cls, max_retries: int) -> None:
        """
        Use this method when setting the number of retries
        from commandline options by callback
        """
        cls.max_retries = max_retries
