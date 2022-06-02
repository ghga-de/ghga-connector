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
from typing import Any, Callable, Optional

from ghga_connector.core.exceptions import (
    FatalError,
    KnownError,
    MaxRetriesReached,
    RetryAbortException,
)


class WithRetry:
    """Class decorator providing common retry logic"""

    max_retries: Optional[int] = None

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
            if WithRetry.max_retries is None:
                raise ValueError("max_retries was not set")

            error_causes: list[KnownError] = []
            # try calling decorated function at least once
            for i in range(WithRetry.max_retries + 1):
                try:
                    func = self.func(*args, **kwargs)
                    return func
                except KnownError as error:  # unkown errors are raised immediately
                    if isinstance(error, FatalError):
                        if len(error_causes) > 0:
                            raise RetryAbortException(
                                func_name=self.func.__name__, causes=error_causes
                            ) from error
                        raise error
                    error_causes.append(error)
                    # Use exponential backoff for retries
                    backoff_factor = 0.5
                    exponential_backoff = backoff_factor * (2 ** (i))
                    time.sleep(exponential_backoff)
            raise MaxRetriesReached(func_name=self.func.__name__, causes=error_causes)

        return retry()

    @classmethod
    def set_retries(cls, max_retries: int) -> None:
        """
        Setting max retries with sanity checks
        """
        if max_retries < 0:
            raise ValueError(
                f"Invalid, negative number provided for max_retries: {max_retries}"
            )
        if cls.max_retries is not None:
            raise ValueError("max_retries is already set")
        cls.max_retries = max_retries
