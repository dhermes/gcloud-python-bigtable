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
from gcloud_bigtable._helpers import _to_bytes
from gcloud_bigtable.column_family import ColumnFamily
from gcloud_bigtable.column_family import _gc_rule_from_pb
from gcloud_bigtable.row import Row
from gcloud_bigtable.row_data import PartialRowData
from gcloud_bigtable.row_data import PartialRowsData


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

    :type table_id: str
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

        :rtype: str
        :returns: The table name.
        """
        return self.cluster.name + '/tables/' + self.table_id

    def column_family(self, column_family_id, gc_rule=None):
        """Factory to create a column family associated with this table.

        :type column_family_id: str
        :param column_family_id: The ID of the column family. Must be of the
                                 form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

        :type gc_rule: :class:`.column_family.GarbageCollectionRule`,
                       :class:`.column_family.GarbageCollectionRuleUnion` or
                       :class:`.column_family.GarbageCollectionRuleIntersection`
        :param gc_rule: (Optional) The garbage collection settings for this
                        column family.

        :rtype: :class:`.column_family.ColumnFamily`
        :returns: A column family owned by this table.
        """
        return ColumnFamily(column_family_id, self, gc_rule=gc_rule)

    def row(self, row_key, filter_=None):
        """Factory to create a row associated with this table.

        :type row_key: bytes
        :param row_key: The key for the row being created.

        :type filter_: :class:`.RowFilter`,
                       :class:`.RowFilterChain`,
                       :class:`.RowFilterUnion`, or
                       :class:`.ConditionalRowFilter`
        :param filter_: (Optional) Filter to be used for conditional mutations.
                        See :class:`.Row` for more details.

        :rtype: :class:`.Row`
        :returns: A row owned by this table.
        """
        return Row(row_key, self, filter_=filter_)

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

        :type initial_split_keys: list
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

        :type new_table_id: str
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

    def read_row(self, row_key, filter_=None, timeout_seconds=None):
        """Read a single row from this table.

        :type row_key: bytes
        :param row_key: The key of the row to read from.

        :type filter_: :class:`.row.RowFilter`, :class:`.row.RowFilterChain`,
                       :class:`.row.RowFilterUnion` or
                       :class:`.row.ConditionalRowFilter`
        :param filter_: (Optional) The filter to apply to the contents of the
                        row. If unset, returns the entire row.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.

        :rtype: :class:`.PartialRowData`
        :returns: The contents of the row.
        :raises: :class:`ValueError <exceptions.ValueError>` if a commit row
                 chunk is never encountered.
        """
        request_pb = _create_row_request(self.name, row_key=row_key,
                                         filter_=filter_)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response_iterator = self.client.data_stub.ReadRows(request_pb,
                                                           timeout_seconds)
        # We expect an iterator of `data_messages_pb2.ReadRowsResponse`
        result = PartialRowData(row_key)
        for read_rows_response in response_iterator:
            result.update_from_read_rows(read_rows_response)

        # Make sure the result was committed by the back-end.
        if not result.committed:
            raise ValueError('The row remains partial / is not committed.')
        return result

    def read_rows(self, start_key=None, end_key=None,
                  allow_row_interleaving=None, limit=None, filter_=None,
                  timeout_seconds=None):
        """Read rows from this table.

        :type start_key: bytes
        :param start_key: (Optional) The beginning of a range of row keys to
                          read from. The range will include ``start_key``. If
                          left empty, will be interpreted as the empty string.

        :type end_key: bytes
        :param end_key: (Optional) The end of a range of row keys to read from.
                        The range will not include ``end_key``. If left empty,
                        will be interpreted as an infinite string.

        :type filter_: :class:`.row.RowFilter`, :class:`.row.RowFilterChain`,
                       :class:`.row.RowFilterUnion` or
                       :class:`.row.ConditionalRowFilter`
        :param filter_: (Optional) The filter to apply to the contents of the
                        specified row(s). If unset, reads every column in
                        each row.

        :type allow_row_interleaving: bool
        :param allow_row_interleaving: (Optional) By default, rows are read
                                       sequentially, producing results which
                                       are guaranteed to arrive in increasing
                                       row order. Setting
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

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.

        :rtype: :class:`.PartialRowsData`
        :returns: A :class:`.PartialRowsData` convenience wrapper for consuming
                  the streamed results.
        """
        request_pb = _create_row_request(
            self.name, start_key=start_key, end_key=end_key, filter_=filter_,
            allow_row_interleaving=allow_row_interleaving, limit=limit)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response_iterator = self.client.data_stub.ReadRows(request_pb,
                                                           timeout_seconds)
        # We expect an iterator of `data_messages_pb2.ReadRowsResponse`
        return PartialRowsData(response_iterator)

    def sample_row_keys(self, timeout_seconds=None):
        """Read a sample of row keys in the table.

        The returned row keys will delimit contiguous sections of the table of
        approximately equal size, which can be used to break up the data for
        distributed tasks like mapreduces.

        The elements in the iterator are a SampleRowKeys response and they have
        the properties ``offset_bytes`` and ``row_key``. They occur in sorted
        order. The table might have contents before the first row key in the
        list and after the last one, but a key containing the empty string
        indicates "end of table" and will be the last response given, if
        present.

        .. note::

            Row keys in this list may not have ever been written to or read
            from, and users should therefore not make any assumptions about the
            row key structure that are specific to their use case.

        The ``offset_bytes`` field on a response indicates the approximate
        total storage space used by all rows in the table which precede
        ``row_key``. Buffering the contents of all rows between two subsequent
        samples would require space roughly equal to the difference in their
        ``offset_bytes`` fields.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.

        :rtype: :class:`grpc.framework.alpha._reexport._CancellableIterator`
        :returns: A cancel-able iterator. Can be consumed by calling ``next()``
                  or by casting to a :class:`list` and can be cancelled by
                  calling ``cancel()``.
        """
        request_pb = data_messages_pb2.SampleRowKeysRequest(
            table_name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response_iterator = self.client.data_stub.SampleRowKeys(
            request_pb, timeout_seconds)
        return response_iterator


def _create_row_request(table_name, row_key=None, start_key=None, end_key=None,
                        filter_=None, allow_row_interleaving=None, limit=None):
    """Creates a request to read rows in a table.

    :type table_name: str
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

    :type filter_: :class:`.row.RowFilter`, :class:`.row.RowFilterChain`,
                   :class:`.row.RowFilterUnion` or
                   :class:`.row.ConditionalRowFilter`
    :param filter_: (Optional) The filter to apply to the contents of the
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
    if filter_ is not None:
        request_kwargs['filter'] = filter_.to_pb()
    if allow_row_interleaving is not None:
        request_kwargs['allow_row_interleaving'] = allow_row_interleaving
    if limit is not None:
        request_kwargs['num_rows_limit'] = limit

    return data_messages_pb2.ReadRowsRequest(**request_kwargs)
