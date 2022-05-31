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
from typing import Any, Callable

from ghga_connector.core.constants import MAX_RETRIES
from ghga_connector.core.exceptions import (
    BadResponseCodeError,
    MaxRetriesReached,
    RequestFailedError,
)


class Retry:
    """Class decorator providing common retry logic"""

    num_retries: int = MAX_RETRIES

    def __init__(self, func: Callable) -> None:
        self.func = func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        def retry():
            # try calling decorated function at least once
            for i in range(Retry.num_retries + 1):
                try:
                    func = self.func(*args, **kwargs)
                    return func
                except BadResponseCodeError as exception:
                    print(
                        f"""Attempt {i+1} for {self.func.__name__}
                        returned unexpected return code: '{exception.response_code}'"""
                    )
                    if exception.response_code in range(400, 600):
                        raise exception
                except RequestFailedError as exception:
                    print(
                        f"""Attempt {i+1} for {self.func.__name__}
                        failed due to: '{exception.__cause__}'"""
                    )
            raise MaxRetriesReached(self.func.__name__)

        return retry()

    @classmethod
    def set_retries(cls, num_retries: int) -> None:
        """
        Use this method when setting the number of retries from commandline options by callback
        """
        cls.num_retries = num_retries
