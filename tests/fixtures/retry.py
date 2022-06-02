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


@pytest.fixture
def max_retries() -> Generator[int, None, None]:
    """
    Fixture dealing with cleanup for all tests touching functions
    annotated with the 'WithRetry' class decorator.
    Those tests need to request this fixture and use 'WithRetry.set_retries'
    with the yielded value as argument.
    As some tests call into functions that set 'WithRetry.maxretries'
    and it is not allowed to be set if it already has a non 'None' value,
    this is required for now
    """
    max_retries = 0
    yield max_retries
    WithRetry.max_retries = None
