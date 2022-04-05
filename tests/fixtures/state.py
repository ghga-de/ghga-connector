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
#     """_
#     File State class for available files
#     """

#     def __init__(
#         self,
#         file_id: str,
#         grouping_label: str,
#         file_path: Path,
#         populate_storage: bool = True,
#     ):
#         """
#         Initialize file state and create imputed attributes.
#         Set populate_storage to true in order to upload them to the localstack storage
#         """
#         self.file_id = file_id
#         self.grouping_label = grouping_label
#         self.file_path = file_path
#         self.populate_storage = populate_storage

#         # computed attributes:
#         with open(self.file_path, "rb") as file:
#             self.content = file.read()

#         filename, file_extension = os.path.splitext(self.file_path)

#         self.storage_objects: List[ObjectFixture] = []
#         if self.populate_storage:
#             self.storage_objects.append(
#                 ObjectFixture(
#                     file_path=self.file_path,
#                     bucket_id="",
#                     object_id=str(self.file_id),
#                 )
#             )


# FILES: Dict[str, FileState] = {
#     "in_registry_in_storage": FileState(
#         file_id=get_file_id_example(0),
#         grouping_label=get_study_id_example(0),
#         file_path=TEST_FILE_PATHS[0],
#         populate_storage=True,
#     ),
#     "in_registry_not_in_storage": FileState(
#         file_id=get_file_id_example(1),
#         grouping_label=get_study_id_example(1),
#         file_path=TEST_FILE_PATHS[1],
#         populate_storage=False,
#     ),
#     "not_in_registry_not_in_storage": FileState(
#         file_id=get_file_id_example(2),
#         grouping_label=get_study_id_example(2),
#         file_path=TEST_FILE_PATHS[2],
#         populate_storage=False,
#     ),
# }
