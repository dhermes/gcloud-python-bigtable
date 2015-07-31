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


from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._helpers import _timestamp_to_microseconds
from gcloud_bigtable._helpers import _to_bytes
from gcloud_bigtable._helpers import make_stub
from gcloud_bigtable.constants import DATA_API_HOST
from gcloud_bigtable.constants import DATA_API_PORT
from gcloud_bigtable.constants import DATA_STUB_FACTORY


_MAX_MUTATIONS = 100000


class Row(object):
    """Representation of a Google Cloud Bigtable Column Row.

    .. note::

        A :class:`Row` accumulates mutations locally via the :meth:`set_cell`,
        :meth:`delete`, :meth:`delete_cell` and :meth:`delete_cells` methods.
        To actually send these mutations to the Google Cloud Bigtable API, you
        must call :meth:`commit`.

    :type row_key: bytes (or string)
    :param row_key: The key for the current row.

    :type table: :class:`.table.Table`
    :param table: The table that owns the row.
    """

    ALL_COLUMNS = object()
    """Sentinel value used to indicate all columns in a column family."""

    def __init__(self, row_key, table):
        self._row_key = _to_bytes(row_key)
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

    @property
    def client(self):
        """Getter for row's client.

        :rtype: :class:`.client.Client`
        :returns: The client that owns this row.
        """
        return self.table.client

    @property
    def timeout_seconds(self):
        """Getter for row's default timeout seconds.

        :rtype: integer
        :returns: The timeout seconds default.
        """
        return self.table.timeout_seconds

    def set_cell(self, column_family_id, column, value, timestamp=None):
        """Sets a value in this row.

        The cell is determined by the ``row_key`` of the :class:`Row` and the
        ``column``. The ``column`` must be in an existing
        :class:`.column_family.ColumnFamily` (as determined by
        ``column_family_id``).

        .. note::

            This method adds a mutation to the accumulated mutations on this
            :class:`Row`, but does not make an API request. To actually
            send an API request (with the mutations) to the Google Cloud
            Bigtable API, call :meth:`commit`.

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
        column = _to_bytes(column)
        value = _to_bytes(value)
        if timestamp is None:
            # Use -1 for current Bigtable server time.
            timestamp_micros = -1
        else:
            timestamp_micros = _timestamp_to_microseconds(timestamp)

        mutation_val = data_pb2.Mutation.SetCell(
            family_name=column_family_id,
            column_qualifier=column,
            timestamp_micros=timestamp_micros,
            value=value,
        )
        mutation_pb = data_pb2.Mutation(set_cell=mutation_val)
        self._pb_mutations.append(mutation_pb)

    def delete(self):
        """Deletes this row from the table.

        .. note::

            This method adds a mutation to the accumulated mutations on this
            :class:`Row`, but does not make an API request. To actually
            send an API request (with the mutations) to the Google Cloud
            Bigtable API, call :meth:`commit`.
        """
        mutation_val = data_pb2.Mutation.DeleteFromRow()
        mutation_pb = data_pb2.Mutation(delete_from_row=mutation_val)
        self._pb_mutations.append(mutation_pb)

    def delete_cell(self, column_family_id, column, start=None, end=None):
        """Deletes cell in this row.

        .. note::

            This method adds a mutation to the accumulated mutations on this
            :class:`Row`, but does not make an API request. To actually
            send an API request (with the mutations) to the Google Cloud
            Bigtable API, call :meth:`commit`.

        :type column_family_id: string
        :param column_family_id: The column family that contains the column
                                 or columns with cells being deleted.

        :type column: bytes (or string)
        :param column: The column within the column family that will have a
                       cell deleted.

        :type start: :class:`datetime.datetime`
        :param start: (Optional) The (inclusive) lower bound of the timestamp
                      range within which cells should be deleted. If omitted,
                      defaults to Unix epoch.

        :type end: :class:`datetime.datetime`
        :param end: (Optional) The (exclusive) upper bound of the timestamp
                    range within which cells should be deleted. If omitted,
                    defaults to "infinity" (no upper bound).
        """
        self.delete_cells(column_family_id, [column], start=start, end=end)

    def delete_cells(self, column_family_id, columns, start=None, end=None):
        """Deletes cells in this row.

        .. note::

            This method adds a mutation to the accumulated mutations on this
            :class:`Row`, but does not make an API request. To actually
            send an API request (with the mutations) to the Google Cloud
            Bigtable API, call :meth:`commit`.

        :type column_family_id: string
        :param column_family_id: The column family that contains the column
                                 or columns with cells being deleted.

        :type columns: iterable of bytes (or strings) or object
        :param columns: The columns within the column family that will have
                        cells deleted. If :attr:`Row.ALL_COLUMNS` is used then
                        the entire column family will be deleted from the row.

        :type start: :class:`datetime.datetime`
        :param start: (Optional) The (inclusive) lower bound of the timestamp
                      range within which cells should be deleted. If omitted,
                      defaults to Unix epoch.

        :type end: :class:`datetime.datetime`
        :param end: (Optional) The (exclusive) upper bound of the timestamp
                    range within which cells should be deleted. If omitted,
                    defaults to "infinity" (no upper bound).
        """
        if columns is self.ALL_COLUMNS:
            mutation_val = data_pb2.Mutation.DeleteFromFamily(
                family_name=column_family_id,
            )
            mutation_pb = data_pb2.Mutation(delete_from_family=mutation_val)
            self._pb_mutations.append(mutation_pb)
        else:
            timestamp_range_kwargs = {}
            if start is not None:
                timestamp_range_kwargs['start_timestamp_micros'] = (
                    _timestamp_to_microseconds(start))
            if end is not None:
                timestamp_range_kwargs['end_timestamp_micros'] = (
                    _timestamp_to_microseconds(end))
            # NOTE: If start == end == None, this will just be empty. It seems
            #       this is equivalent to not passing ``time_range`` to the
            #       ``DeleteFromColumn`` constructor (we are assuming that).
            time_range = data_pb2.TimestampRange(**timestamp_range_kwargs)
            to_append = []
            for column in columns:
                column = _to_bytes(column)
                mutation_val = data_pb2.Mutation.DeleteFromColumn(
                    family_name=column_family_id,
                    column_qualifier=column,
                    time_range=time_range,
                )
                mutation_pb = data_pb2.Mutation(
                    delete_from_column=mutation_val)
                to_append.append(mutation_pb)

            # We don't add the mutations until all columns have been
            # processed without error.
            self._pb_mutations.extend(to_append)

    def commit(self, timeout_seconds=None):
        """Makes a ``MutateRow`` API request.

        If no mutations have been created in the row, no request is made.

        Mutations are applied atomically and in order, meaning that earlier
        mutations can be masked / negated by later ones. Cells already present
        in the row are left unchanged unless explicitly changed by a mutation.

        After committing the accumulated mutations, resets the local
        mutations to an empty list.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on row.

        :raises: :class:`ValueError` if the number of mutations exceeds the
                 ``_MAX_MUTATIONS``.
        """
        num_mutations = len(self._pb_mutations)
        if num_mutations == 0:
            return
        if num_mutations > _MAX_MUTATIONS:
            raise ValueError('%d total mutations exceed the maximum allowable '
                             '%d.' % (num_mutations, _MAX_MUTATIONS))
        request_pb = messages_pb2.MutateRowRequest(
            table_name=self.table.name,
            row_key=self.row_key,
            mutations=self._pb_mutations,
        )
        stub = make_stub(self.client, DATA_STUB_FACTORY,
                         DATA_API_HOST, DATA_API_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.MutateRow.async(request_pb, timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`.
            response.result()

        # Reset mutations after commit-ing request.
        self._pb_mutations = []


class RowFilter(object):
    """Basic filter to apply to cells in a row.

    These values can be combined via :class:`RowFilterChain`,
    :class:`RowFilterInterleave` and :class:`RowFilterTernary`.

    The regex filters must be valid RE2 patterns. See
    https://github.com/google/re2/wiki/Syntax for the accepted syntax.

    .. note::

        At most one of the keyword arguments can be specified at once.

    .. note::

        For ``bytes`` regex filters (``row_key``, ``column_qualifier`` and
        ``value``), special care need be used with the expression used. Since
        each of these properties can contain arbitrary bytes, the ``\\C``
        escape sequence must be used if a true wildcard is desired. The ``.``
        character will not match the new line character ``\\n``, which may be
        present in a binary value.

    :type row_key_regex_filter: bytes (or string)
    :param row_key_regex_filter: A regular expression (RE2) to match cells from
                                 rows with row keys that satisfy this regex.
                                 For a ``CheckAndMutateRowRequest``, this
                                 filter is unnecessary since the row key is
                                 already specified.

    :type family_name_regex_filter: string
    :param family_name_regex_filter: A regular expression (RE2) to match cells
                                     from columns in a given column family. For
                                     technical reasons, the regex must not
                                     contain the ':' character, even if it is
                                     not being uses as a literal.

    :type column_qualifier_regex_filter: bytes (or string)
    :param column_qualifier_regex_filter: A regular expression (RE2) to match
                                          cells from column that match this
                                          regex (irrespective of column
                                          family).

    :type value_regex_filter: bytes (or string)
    :param value_regex_filter: A regular expression (RE2) to match cells with
                               values that match this regex.

    :type cells_per_row_offset_filter: integer
    :param cells_per_row_offset_filter: Skips the first N cells of the row.

    :type cells_per_row_limit_filter: integer
    :param cells_per_row_limit_filter: Matches only the first N cells of the
                                       row.

    :type cells_per_column_limit_filter: integer
    :param cells_per_column_limit_filter: Matches only the most recent N cells
                                          within each column. This filters a
                                          (family name, column) pair, based on
                                          timestamps of each cell.

    :type row_sample_filter: float
    :param row_sample_filter: Non-deterministic filter. Matches all cells from
                              a row with probability p, and matches no cells
                              from the row with probability 1-p. (Here, the
                              probability p is ``row_sample_filter``.)

    :type strip_value_transformer: boolean
    :param strip_value_transformer: If ``True``, replaces each cell's value
                                    with the empty string. As the name
                                    indicates, this is more useful as a
                                    transformer than a generic query / filter.

    :raises: :class:`TypeError` if not exactly one value set in the
             constructor.
    """

    def __init__(self,
                 row_key_regex_filter=None,
                 family_name_regex_filter=None,
                 column_qualifier_regex_filter=None,
                 value_regex_filter=None,
                 cells_per_row_offset_filter=None,
                 cells_per_row_limit_filter=None,
                 cells_per_column_limit_filter=None,
                 row_sample_filter=None,
                 strip_value_transformer=None):
        values_set = (
            int(row_key_regex_filter is not None) +
            int(family_name_regex_filter is not None) +
            int(column_qualifier_regex_filter is not None) +
            int(value_regex_filter is not None) +
            int(cells_per_row_offset_filter is not None) +
            int(cells_per_row_limit_filter is not None) +
            int(cells_per_column_limit_filter is not None) +
            int(row_sample_filter is not None) +
            int(strip_value_transformer is not None)
        )
        if values_set != 1:
            raise TypeError('Exactly one value must be set in a row filter')

        self.row_key_regex_filter = row_key_regex_filter
        self.family_name_regex_filter = family_name_regex_filter
        self.column_qualifier_regex_filter = column_qualifier_regex_filter
        self.value_regex_filter = value_regex_filter
        self.cells_per_row_offset_filter = cells_per_row_offset_filter
        self.cells_per_row_limit_filter = cells_per_row_limit_filter
        self.cells_per_column_limit_filter = cells_per_column_limit_filter
        self.row_sample_filter = row_sample_filter
        self.strip_value_transformer = strip_value_transformer
