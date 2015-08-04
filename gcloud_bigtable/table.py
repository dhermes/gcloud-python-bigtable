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


from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as messages_pb2)
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
