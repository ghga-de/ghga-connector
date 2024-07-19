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

"""This file contains general utility api calls"""

import httpx


def is_service_healthy(api_url: str, *, timeout_in_seconds: int = 5) -> bool:
    """Check if the corresponding health endpoint is available"""
    # Adjust url so the the health endpoint is actually called
    if not api_url.endswith("/health"):
        if not api_url.endswith("/"):
            api_url += "/"
        api_url += "health"

    return check_url(api_url=api_url, timeout_in_seconds=timeout_in_seconds)


def check_url(api_url: str, *, timeout_in_seconds: int = 5) -> bool:
    """Checks, if an url is reachable within a certain time"""
    try:
        httpx.get(url=api_url, timeout=timeout_in_seconds)
    except httpx.RequestError:
        return False
    return True
