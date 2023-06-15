# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Handling session initialization for requests"""
import httpx


class HttpxClient:
    """Helper class to make max_retries user configurable"""

    client: httpx.Client

    @classmethod
    def configure(cls, max_retries: int):
        """Configure client with exponential backoff retry (using httpx's 0.5 default)"""

        # can't be negative - should we log this?
        max_retries = max(0, max_retries)
        transport = httpx.HTTPTransport(retries=max_retries)
        cls.client = httpx.Client(transport=transport)

    @classmethod
    def get(cls, *args, **kwargs):
        """Delegate to session method"""
        return cls.client.get(*args, **kwargs)

    @classmethod
    def patch(cls, *args, **kwargs):
        """Delegate to session method"""
        return cls.client.patch(*args, **kwargs)

    @classmethod
    def post(cls, *args, **kwargs):
        """Delegate to session method"""
        return cls.client.post(*args, **kwargs)

    @classmethod
    def put(cls, *args, **kwargs):
        """Delegate to session method"""
        return cls.client.put(*args, **kwargs)
