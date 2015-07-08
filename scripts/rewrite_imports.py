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

"""Build script for rewriting imports for protobuf generated modules.

Intended to be used for Google Cloud BigTable protos (google/bigtable/v1)
and the dependent modules (google/api and google/protobuf).
"""

import glob


IMPORT_TEMPLATE = 'import %s'
IMPORT_FROM_TEMPLATE = 'from %s import '
PROTOBUF_IMPORT_TEMPLATE = 'from google.protobuf import %s '
REPLACE_PROTOBUF_IMPORT_TEMPLATE = 'from gcloud_bigtable._generated import %s '
REPLACEMENTS = {
    'google.api': 'gcloud_bigtable._generated',
    'google.bigtable.admin.cluster.v1': 'gcloud_bigtable._generated',
    'google.bigtable.admin.table.v1': 'gcloud_bigtable._generated',
    'google.bigtable.v1': 'gcloud_bigtable._generated',
    'google.longrunning': 'gcloud_bigtable._generated',
    'google.rpc': 'gcloud_bigtable._generated',
}
GOOGLE_PROTOBUF_CUSTOM = (
    'any_pb2',
    'duration_pb2',
    'empty_pb2',
    'timestamp_pb2',
)


def transform_line(line):
    """Transforms an import line in a PB2 module.

    If the line is not an import of one of the packages in
    ``REPLACEMENTS`` or ``GOOGLE_PROTOBUF_CUSTOM``, does nothing and returns
    the original. Otherwise it replaces the package matched with our local
    package or directly rewrites the custom ``google.protobuf`` import
    statement.

    :type line: string
    :param line: The line to be transformed.

    :rtype: string
    :returns: The transformed line.
    """
    for old_module, new_module in REPLACEMENTS.iteritems():
        import_from_statement = IMPORT_FROM_TEMPLATE % (old_module,)
        if line.startswith(import_from_statement):
            new_import_from_statement = IMPORT_FROM_TEMPLATE % (new_module,)
            # Only replace the first instance of the import statement.
            return line.replace(import_from_statement,
                                new_import_from_statement, 1)

        # If the line doesn't start with a "from * import *" statement, it
        # may still contain a "import * ..." statement.
        import_statement = IMPORT_TEMPLATE % (old_module,)
        if import_statement in line:
            new_import_statement = IMPORT_TEMPLATE % (new_module,)
            # Only replace the first instance of the import statement.
            return line.replace(import_statement,
                                new_import_statement, 1)

    for custom_protobuf_module in GOOGLE_PROTOBUF_CUSTOM:
        import_from_statement = PROTOBUF_IMPORT_TEMPLATE % (
            custom_protobuf_module,)
        if line.startswith(import_from_statement):
            new_import_from_statement = REPLACE_PROTOBUF_IMPORT_TEMPLATE % (
                custom_protobuf_module,)
            # Only replace the first instance of the import statement.
            return line.replace(import_from_statement,
                                new_import_from_statement, 1)

        # If the line doesn't start with a "from * import *" statement, it
        # may still contain a "import * ..." statement.
        import_statement = IMPORT_TEMPLATE % (
            'google.protobuf.' + custom_protobuf_module,)
        if import_statement in line:
            new_import_statement = IMPORT_TEMPLATE % (
                'gcloud_bigtable._generated.' + custom_protobuf_module,)
            # Only replace the first instance of the import statement.
            return line.replace(import_statement,
                                new_import_statement, 1)

    # If no matches, there is nothing to transform.
    return line


def rewrite_file(filename):
    """Rewrites a given PB2 modules.

    :type filename: string
    :param filename: The name of the file to be rewritten.
    """
    with open(filename, 'rU') as file_obj:
        content_lines = file_obj.read().split('\n')

    new_content = []
    for line in content_lines:
        new_content.append(transform_line(line))

    with open(filename, 'w') as file_obj:
        file_obj.write('\n'.join(new_content))


def main():
    """Rewrites all PB2 files."""
    pb2_files = glob.glob('gcloud_bigtable/_generated/*pb2.py')
    for filename in pb2_files:
        rewrite_file(filename)


if __name__ == '__main__':
    main()
