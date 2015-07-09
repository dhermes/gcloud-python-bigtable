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

"""Checking that protobuf generated modules import correctly."""

import os


BASE_DIRECTORY = 'gcloud_bigtable/_generated'


def main():
    """Import all PB2 files."""
    print('>>> import gcloud_bigtable._generated')
    import gcloud_bigtable._generated
    for directory, _, files in os.walk(BASE_DIRECTORY):
        import_parts = directory.split(os.path.sep)
        from_package = '.'.join(import_parts)
        for filename in files:
            if filename.endswith('pb2.py'):
                module_name, _ = os.path.splitext(filename)
                print('>>> from %s import %s' % (from_package, module_name))
                _ = __import__(from_package, fromlist=[module_name])


if __name__ == '__main__':
    main()
