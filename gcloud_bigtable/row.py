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

"""User friendly container for Google Cloud Bigtable Row."""


import six


class Row(object):
    """Representation of a Google Cloud Bigtable Column Row.

    :type row_key: bytes (or string)
    :param row_key: The key for the current row.

    :type table: :class:`.table.Table`
    :param table: The table that owns the row.

    :raises: :class:`TypeError` if the ``row_key`` is not bytes or string.
    """

    def __init__(self, row_key, table):
        if isinstance(row_key, six.text_type):
            row_key = row_key.encode('utf-8')
        if not isinstance(row_key, bytes):
            raise TypeError('Row key must be bytes.')
        self._row_key = row_key
        self._table = table
        self._pb_mutations = []

    @property
    def table(self):
        """Getter for row's table.

        :rtype: :class:`.table.Table`
        :returns: The table stored on the row.
        """
        return self._table

    @property
    def row_key(self):
        """Getter for row's key.

        :rtype: bytes
        :returns: The key for the row.
        """
        return self._row_key
