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

"""Utils for Fixture handling"""

from enum import Enum
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()

# fmt: off
class UploadState(Enum):

    """
    The current upload state. Can be registered (no information),
    pending (the user has requested an upload url),
    uploaded (the user has confirmed the upload),
    or registered (the file has been registered with the internal-file-registry).
    """

    REGISTERED = "registered"
    PENDING = "pending"
    UPLOADED = "uploaded"
    COMPLETED = "completed"
# fmt: on
