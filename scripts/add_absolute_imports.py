# Copyright 2015 Google Inc. All rights reserved.
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

"""Build script for adding absolute_import to protobuf generated modules.

Simply iterators through all *pb2.py files and adds
    from __future__ import absolute_import
as the first line.
"""

import os


BASE_DIRECTORY = 'gcloud_bigtable/_generated'
ABSOLUTE_IMPORT_LINE = 'from __future__ import absolute_import\n'


def main():
    """Add absolute import line to all PB2 files."""
    for directory, _, files in os.walk(BASE_DIRECTORY):
        for filename in files:
            if filename.endswith('pb2.py'):
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r') as file_obj:
                    contents = file_obj.read()
                with open(file_path, 'w') as file_obj:
                    file_obj.write(ABSOLUTE_IMPORT_LINE)
                    file_obj.write(contents)


if __name__ == '__main__':
    main()
