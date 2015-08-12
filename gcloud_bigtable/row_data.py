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

"""Container for Google Cloud Bigtable Cells and Streaming Row Contents."""


import copy


class Cell(object):
    """Representation of a Google Cloud Bigtable Cell.

    :type value: bytes
    :param value: The value stored in the cell.

    :type timestamp: :class:`datetime.datetime`
    :param timestamp: The timestamp when the cell was stored.
    """

    def __init__(self, value, timestamp):
        self.value = value
        self.timestamp = timestamp

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (other.value == self.value and
                other.timestamp == self.timestamp)

    def __ne__(self, other):
        return not self.__eq__(other)


class PartialRowData(object):
    """Representation of partial row in a Google Cloud Bigtable Table.

    These are expected to be updated directly from a
    :class:`._generated.bigtable_service_messages_pb2.ReadRowsResponse`
    """

    def __init__(self):
        self._row_key = None
        self._cells = {}

    @property
    def cells(self):
        """Property returning all the cells accumulated on this partial row.

        :rtype: list
        :returns: List of the :class:`Cell` objects accumulated.
        """
        return copy.deepcopy(self._cells)

    def clear(self):
        """Clears all cells that have been added."""
        self._cells.clear()
