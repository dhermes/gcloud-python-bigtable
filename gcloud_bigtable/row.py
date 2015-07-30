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

from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
from gcloud_bigtable._helpers import EPOCH as _EPOCH


class Row(object):
    """Representation of a Google Cloud Bigtable Column Row.

    :type row_key: bytes (or string)
    :param row_key: The key for the current row.

    :type table: :class:`.table.Table`
    :param table: The table that owns the row.

    :raises: :class:`TypeError` if the ``row_key`` is not bytes or string.
    """

    ALL_COLUMNS = object()
    """Sentinel value used to indicate all columns in a column family."""

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

    def set_cell(self, column_family_id, column, value, timestamp=None):
        """Sets a value in this row.

        The cell is determined by the ``row_key`` of the :class:`Row` and the
        ``column``. The ``column`` must be in an existing
        :class:`.column_family.ColumnFamily`.

        :type column_family_id: string
        :param column_family_id: The column family that contains the column.

        :type column: bytes (or string)
        :param column: The column within the column family where the cell
                       is located.

        :type value: bytes
        :param value: The value to set in the cell.

        :type timestamp: :class:`datetime.datetime`
        :param timestamp: (Optional) The timestamp of the operation.

        :raises: :class:`TypeError` if the ``value`` is not bytes.
        """
        if isinstance(column, six.text_type):
            column = column.encode('utf-8')
        if not isinstance(value, bytes):
            raise TypeError('Value for a cell must be bytes.')
        if timestamp is None:
            # Use -1 for current Bigtable server time.
            timestamp_micros = -1
        else:
            timestamp_seconds = (timestamp - _EPOCH).total_seconds()
            timestamp_micros = int(10**6 * timestamp_seconds)
            # Truncate to millisecond resolution, since the the only value of
            # `._generated.bigtable_table_data_pb2.Table.TimestampGranularity`
            # is `._generated.bigtable_table_data_pb2.Table.MILLIS`.
            timestamp_micros -= (timestamp_micros % 1000)

        mutation_val = data_pb2.Mutation.SetCell(
            family_name=column_family_id,
            column_qualifier=column,
            timestamp_micros=timestamp_micros,
            value=value,
        )
        mutation_pb = data_pb2.Mutation(set_cell=mutation_val)
        self._pb_mutations.append(mutation_pb)

    def delete(self):
        """Deletes this row from the table."""
        mutation_val = data_pb2.Mutation.DeleteFromRow()
        mutation_pb = data_pb2.Mutation(delete_from_row=mutation_val)
        self._pb_mutations.append(mutation_pb)

    def delete_cells(self, column_family_id, columns, start=None, end=None):
        """Deletes cells in this row.

        :type column_family_id: string
        :param column_family_id: The column family that contains the column
                                 or columns with cells being deleted.

        :type columns: iterable of bytes (or strings) or object
        :param columns: The columns within the column family that will have
                        cells deleted. If :attr:`Row.ALL_COLUMNS` is used then
                        the entire column family will be deleted from the row.

        :type start: :class:`datetime.datetime`
        :param start: (Optional) The beginning of the timestamp range within
                      which cells should be deleted.

        :type end: :class:`datetime.datetime`
        :param end: (Optional) The end of the timestamp range within which
                    cells should be deleted.
        """
        if columns is self.ALL_COLUMNS:
            mutation_val = data_pb2.Mutation.DeleteFromFamily(
                family_name=column_family_id,
            )
            mutation_pb = data_pb2.Mutation(delete_from_family=mutation_val)
            self._pb_mutations.append(mutation_pb)
        else:
            time_range = [start, end]
            raise NotImplementedError(time_range)
