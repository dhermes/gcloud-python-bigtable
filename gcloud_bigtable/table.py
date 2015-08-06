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

"""User friendly container for Google Cloud Bigtable Table."""


from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_service_messages_pb2 as data_messages_pb2)
from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._helpers import _parse_family_pb
from gcloud_bigtable._helpers import _to_bytes
from gcloud_bigtable.column_family import ColumnFamily
from gcloud_bigtable.column_family import _gc_rule_from_pb
from gcloud_bigtable.row import Row


class Table(object):
    """Representation of a Google Cloud Bigtable Table.

    .. note::

        We don't define any properties on a table other than the name. As
        the proto says, in a request:

          The ``name`` field of the Table and all of its ColumnFamilies must
          be left blank, and will be populated in the response.

        This leaves only the ``current_operation`` and ``granularity``
        fields. The ``current_operation`` is only used for responses while
        ``granularity`` is an enum with only one value.

    We can use a :class:`Table` to:

    * :meth:`create` the table
    * :meth:`rename` the table
    * :meth:`delete` the table
    * :meth:`list_column_families` in the table

    :type table_id: string
    :param table_id: The ID of the table.

    :type cluster: :class:`.cluster.Cluster`
    :param cluster: The cluster that owns the table.
    """

    def __init__(self, table_id, cluster):
        self.table_id = table_id
        self._cluster = cluster

    @property
    def cluster(self):
        """Getter for table's cluster.

        :rtype: :class:`.cluster.Cluster`
        :returns: The cluster stored on the table.
        """
        return self._cluster

    @property
    def client(self):
        """Getter for table's client.

        :rtype: :class:`.client.Client`
        :returns: The client that owns this table.
        """
        return self.cluster.client

    @property
    def timeout_seconds(self):
        """Getter for table's default timeout seconds.

        :rtype: int
        :returns: The timeout seconds default stored on the table's client.
        """
        return self._cluster.timeout_seconds

    @property
    def name(self):
        """Table name used in requests.

        .. note::

          This property will not change if ``table_id`` does not, but the
          return value is not cached.

        The table name is of the form

            ``"projects/../zones/../clusters/../tables/{table_id}"``

        :rtype: string
        :returns: The table name.
        """
        return self.cluster.name + '/tables/' + self.table_id

    def column_family(self, column_family_id, gc_rule=None):
        """Factory to create a column family associated with this table.

        :type column_family_id: string
        :param column_family_id: The ID of the column family.

        :type gc_rule: :class:`.column_family.GarbageCollectionRule`,
                       :class:`.column_family.GarbageCollectionRuleUnion` or
                       :class:`.column_family.GarbageCollectionRuleIntersection`
        :param gc_rule: (Optional) The garbage collection settings for this
                        column family.

        :rtype: :class:`.column_family.ColumnFamily`
        :returns: A column family owned by this table.
        """
        return ColumnFamily(column_family_id, self, gc_rule=gc_rule)

    def row(self, row_key):
        """Factory to create a row associated with this table.

        :type row_key: bytes (or string)
        :param row_key: The key for the row being created.

        :rtype: :class:`.row.Row`
        :returns: A row owned by this table.
        """
        return Row(row_key, self)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (other.table_id == self.table_id and
                other.cluster == self.cluster)

    def __ne__(self, other):
        return not self.__eq__(other)

    def create(self, initial_split_keys=None, timeout_seconds=None):
        """Creates this table.

        .. note::

            Though a :class:`._generated.bigtable_table_data_pb2.Table` is also
            allowed (as the ``table`` property) in a create table request, we
            do not support it in this method. As mentioned in the
            :class:`Table` docstring, the name is the only useful property in
            the table proto.

        .. note::

            A create request returns a
            :class:`._generated.bigtable_table_data_pb2.Table` but we don't use
            this response. The proto definition allows for the inclusion of a
            ``current_operation`` in the response, but in example usage so far,
            it seems the Bigtable API does not return any operation.

        :type initial_split_keys: iterable of strings
        :param initial_split_keys: (Optional) List of row keys that will be
                                   used to initially split the table into
                                   several tablets (Tablets are similar to
                                   HBase regions). Given two split keys,
                                   ``"s1"`` and ``"s2"``, three tablets will be
                                   created, spanning the key ranges:
                                   ``[, s1)``, ``[s1, s2)``, ``[s2, )``.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys or [],
            name=self.cluster.name,
            table_id=self.table_id,
        )
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.table_stub.CreateTable.async(request_pb,
                                                            timeout_seconds)
        # We expect a `._generated.bigtable_table_data_pb2.Table`
        response.result()

    def rename(self, new_table_id, timeout_seconds=None):
        """Rename this table.

        .. note::

            This cannot be used to move tables between clusters,
            zones, or projects.

        .. note::

            The Bigtable Table Admin API currently returns

                ``BigtableTableService.RenameTable is not yet implemented``

            when this method is used. It's unclear when this method will
            actually be supported by the API.

        :type new_table_id: string
        :param new_table_id: The new name table ID.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        request_pb = messages_pb2.RenameTableRequest(
            name=self.name,
            new_id=new_table_id,
        )
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.table_stub.RenameTable.async(request_pb,
                                                            timeout_seconds)
        # We expect a `._generated.empty_pb2.Empty`
        response.result()

        self.table_id = new_table_id

    def delete(self, timeout_seconds=None):
        """Delete this table.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        request_pb = messages_pb2.DeleteTableRequest(name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.table_stub.DeleteTable.async(request_pb,
                                                            timeout_seconds)
        # We expect a `._generated.empty_pb2.Empty`
        response.result()

    def list_column_families(self, timeout_seconds=None):
        """Check if this table exists.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.

        :rtype: dictionary with string as keys and
                :class:`.column_family.ColumnFamily` as values
        :returns: List of column families attached to this table.
        :raises: :class:`ValueError <exceptions.ValueError>` if the column
                 family name from the response does not agree with the computed
                 name from the column family ID.
        """
        request_pb = messages_pb2.GetTableRequest(name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.table_stub.GetTable.async(request_pb,
                                                         timeout_seconds)
        # We expect a `._generated.bigtable_table_data_pb2.Table`
        table_pb = response.result()

        result = {}
        for column_family_id, value_pb in table_pb.column_families.items():
            gc_rule = _gc_rule_from_pb(value_pb.gc_rule)
            column_family = self.column_family(column_family_id,
                                               gc_rule=gc_rule)
            if column_family.name != value_pb.name:
                raise ValueError('Column family name %s does not agree with '
                                 'name from request: %s.' % (
                                     column_family.name, value_pb.name))
            result[column_family_id] = column_family
        return result

    def read_row(self, row_key, filter=None, timeout_seconds=None):
        """Read a single row from this table.

        :type row_key: bytes
        :param row_key: The key of the row to read from.

        :type filter: :class:`.row.RowFilter`, :class:`.row.RowFilterChain`,
                      :class:`.row.RowFilterUnion` or
                      :class:`.row.ConditionalRowFilter`
        :param filter: (Optional) The filter to apply to the contents of the
                       row. If unset, returns the entire row.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.

        :rtype: dict
        :returns: The contents of the row. Returns as a dictionary of column
                  families, each of which holds a dictionary of columns. Each
                  column contains a list of cells modified. Each cell is
                  represented with a two-tuple with the value (in bytes) and
                  the timestamp for the cell. For example:

                  .. code:: python

                      {
                          u'col-fam-id': {
                              b'col-name1': [
                                  (b'cell-val', datetime.datetime(...)),
                                  (b'cell-val-newer', datetime.datetime(...)),
                              ],
                              b'col-name2': [
                                  (b'altcol-cell-val', datetime.datetime(...)),
                              ],
                          },
                          u'col-fam-id2': {
                              b'col-name3-but-other-fam': [
                                  (b'foo', datetime.datetime(...)),
                              ],
                          },
                      }
        """
        request_pb = _create_row_request(self.name, row_key=row_key,
                                         filter=filter)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response_iterator = self.client.data_stub.ReadRows(request_pb,
                                                           timeout_seconds)
        # We expect a `data_messages_pb2.ReadRowsResponse`
        read_rows_response, = list(response_iterator)
        # NOTE: We assume read_rows_response.row_key == row_key
        return _parse_row_response(read_rows_response)


def _create_row_request(table_name, row_key=None, start_key=None, end_key=None,
                        filter=None, allow_row_interleaving=None, limit=None):
    """Reads rows in the table.

    :type table_name: string
    :param table_name: The name of the table to read from.

    :type row_key: bytes
    :param row_key: (Optional) The key of a specific row to read from.

    :type start_key: bytes
    :param start_key: (Optional) The beginning of a range of row keys to
                      read from. The range will include ``start_key``. If
                      left empty, will be interpreted as the empty string.

    :type end_key: bytes
    :param end_key: (Optional) The end of a range of row keys to read from.
                    The range will not include ``end_key``. If left empty,
                    will be interpreted as an infinite string.

    :type filter: :class:`.row.RowFilter`, :class:`.row.RowFilterChain`,
                  :class:`.row.RowFilterUnion` or
                  :class:`.row.ConditionalRowFilter`
    :param filter: (Optional) The filter to apply to the contents of the
                   specified row(s). If unset, reads the entire table.

    :type allow_row_interleaving: bool
    :param allow_row_interleaving: (Optional) By default, rows are read
                                   sequentially, producing results which are
                                   guaranteed to arrive in increasing row
                                   order. Setting
                                   ``allow_row_interleaving`` to
                                   :data:`True` allows multiple rows to be
                                   interleaved in the response stream,
                                   which increases throughput but breaks
                                   this guarantee, and may force the
                                   client to use more memory to buffer
                                   partially-received rows.

    :type limit: int
    :param limit: (Optional) The read will terminate after committing to N
                  rows' worth of results. The default (zero) is to return
                  all results. Note that if ``allow_row_interleaving`` is
                  set to :data:`True`, partial results may be returned for
                  more than N rows. However, only N ``commit_row`` chunks
                  will be sent.

    :rtype: :class:`data_messages_pb2.ReadRowsRequest`
    :returns: The ``ReadRowsRequest`` protobuf corresponding to the inputs.
    :raises: :class:`ValueError <exceptions.ValueError>` if both
             ``row_key`` and one of ``start_key`` and ``end_key`` are set
    """
    request_kwargs = {'table_name': table_name}
    if (row_key is not None and
            (start_key is not None or end_key is not None)):
        raise ValueError('Row key and row range cannot be '
                         'set simultaneously')
    if row_key is not None:
        request_kwargs['row_key'] = _to_bytes(row_key)
    if start_key is not None or end_key is not None:
        range_kwargs = {}
        if start_key is not None:
            range_kwargs['start_key'] = _to_bytes(start_key)
        if end_key is not None:
            range_kwargs['end_key'] = _to_bytes(end_key)
        row_range = data_pb2.RowRange(**range_kwargs)
        request_kwargs['row_range'] = row_range
    if filter is not None:
        request_kwargs['filter'] = filter.to_pb()
    if allow_row_interleaving is not None:
        request_kwargs['allow_row_interleaving'] = allow_row_interleaving
    if limit is not None:
        request_kwargs['num_rows_limit'] = limit

    return data_messages_pb2.ReadRowsRequest(**request_kwargs)


def _parse_row_response(read_rows_response_pb):
    """Parses a response from ``ReadRows`` into a dictionary.

    :type read_rows_response_pb: :class:`.data_messages_pb2.ReadRowsResponse`
    :param read_rows_response_pb: A response from ``ReadRows``.

    :rtype: dict
    :returns: The parsed contents of the cells the row. Returned as a
              dictionary of column families, each of which holds a
              dictionary of columns. Each column contains a list of cells
              modified. Each cell is represented with a two-tuple with the
              value (in bytes) and the timestamp for the cell. For example:

              .. code:: python

                  {
                      u'col-fam-id': {
                          b'col-name1': [
                              (b'cell-val', datetime.datetime(...)),
                              (b'cell-val-newer', datetime.datetime(...)),
                          ],
                          b'col-name2': [
                              (b'altcol-cell-val', datetime.datetime(...)),
                          ],
                      },
                      u'col-fam-id2': {
                          b'col-name3-but-other-fam': [
                              (b'foo', datetime.datetime(...)),
                          ],
                      },
                  }

    :raises: :class:`ValueError <exceptions.ValueError>` if a commit row chunk
             occurs anywhere than in the last chunk.
    """
    result = {}
    num_chunks = len(read_rows_response_pb.chunks)
    for i, chunk in enumerate(read_rows_response_pb.chunks):
        if chunk.HasField('commit_row') and chunk.commit_row:
            if i + 1 == num_chunks:
                break
            else:
                raise ValueError('Commit row was not the last chunk')
        elif chunk.HasField('reset_row') and chunk.reset_row:
            result.clear()
        else:
            col_fam_id, local_result = _parse_family_pb(chunk.row_contents)
            col_fam = result.setdefault(col_fam_id, {})
            for col_name, local_cells in local_result.items():
                cells = col_fam.setdefault(col_name, [])
                cells.extend(local_cells)

    return result
