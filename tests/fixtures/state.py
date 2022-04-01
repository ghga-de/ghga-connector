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

"""Test data"""

# import os
# import uuid
# from datetime import datetime, timezone
# from pathlib import Path
# from typing import Dict, List

# from ghga_service_chassis_lib.object_storage_dao_testing import ObjectFixture, calc_md5
# from ghga_service_chassis_lib.utils import TEST_FILE_PATHS
# from pydantic.types import UUID4


# def get_file_id_example(index: int) -> str:
#     "Generate an example file ID."
#     return f"myfile-{index}"


# class FileState:
#     def __init__(
#         self,
#         id: UUID4,
#         file_id: str,
#         grouping_label: str,
#         file_path: Path,
#         populate_db: bool = True,
#         populate_storage: bool = True,
#     ):
#         """
#         Initialize file state and create imputed attributes.
#         You may set `populate_db` or `populate_storage` to `False` to indicate that this
#         file should not be added to the database or the storage respectively.
#         """
#         self.id = id
#         self.file_id = file_id
#         self.grouping_label = grouping_label
#         self.file_path = file_path
#         self.populate_db = populate_db
#         self.populate_storage = populate_storage

#         # computed attributes:
#         with open(self.file_path, "rb") as file:
#             self.content = file.read()

#         filename, file_extension = os.path.splitext(self.file_path)

#         self.md5 = calc_md5(self.content)
#         self.file_info = models.DrsObjectBase(
#             file_id=self.file_id,
#             md5_checksum=self.md5,
#             size=1000,  # not the real size
#             creation_date=datetime.now(timezone.utc),
#             update_date=datetime.now(timezone.utc),
#             format=file_extension,
#         )

#         self.message = {
#             "file_id": self.file_id,
#             "grouping_label": self.grouping_label,
#             "md5_checksum": self.file_info.md5_checksum,
#             "size": self.file_info.size,
#             "creation_date": self.file_info.creation_date.isoformat(),
#             "update_date": self.file_info.update_date.isoformat(),
#             "format": self.file_info.format,
#         }

#         self.storage_objects: List[ObjectFixture] = []
#         if self.populate_storage:
#             self.storage_objects.append(
#                 ObjectFixture(
#                     file_path=self.file_path,
#                     bucket_id=DEFAULT_CONFIG.s3_outbox_bucket_id,
#                     object_id=str(self.file_id),
#                 )
#             )


# FILES: Dict[str, FileState] = {
#     "in_registry_in_storage": FileState(
#         id=uuid.uuid4(),
#         file_id=get_file_id_example(0),
#         grouping_label=get_study_id_example(0),
#         file_path=TEST_FILE_PATHS[0],
#         populate_db=True,
#         populate_storage=True,
#     ),
#     "in_registry_not_in_storage": FileState(
#         id=uuid.uuid4(),
#         file_id=get_file_id_example(1),
#         grouping_label=get_study_id_example(1),
#         file_path=TEST_FILE_PATHS[1],
#         populate_db=True,
#         populate_storage=False,
#     ),
#     "not_in_registry_not_in_storage": FileState(
#         id=uuid.uuid4(),
#         file_id=get_file_id_example(2),
#         grouping_label=get_study_id_example(2),
#         file_path=TEST_FILE_PATHS[2],
#         populate_db=False,
#         populate_storage=False,
#     ),
# }
