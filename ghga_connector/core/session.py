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
"""Handling seesion initialization for requests"""

import requests
from requests.adapters import HTTPAdapter, Retry


def configure_session() -> requests.Session:
    """Configure session with exponential backoff retry"""
    with requests.session() as session:

        retries = Retry(
            total=6, backoff_factor=2, status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)

        session.mount("http://", adapter=adapter)
        session.mount("https://", adapter=adapter)

        return session


SESSION = configure_session()
