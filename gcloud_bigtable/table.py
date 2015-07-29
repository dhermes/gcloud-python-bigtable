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
from gcloud_bigtable._helpers import make_stub
from gcloud_bigtable.constants import TABLE_ADMIN_HOST
from gcloud_bigtable.constants import TABLE_ADMIN_PORT
from gcloud_bigtable.constants import TABLE_STUB_FACTORY


class Table(object):
    """Representation of a Google Cloud Bigtable Table.

    .. note::

        We don't define any properties on a table other than the name. As
        the proto says, in a request:

          "The `name` field of the Table and all of its ColumnFamilies must
           be left blank, and will be populated in the response."

        This leaves only the ``current_operation`` and ``granularity``
        fields. The ``current_operation`` is only used for responses while
        ``granularity`` is an enum with only one value.

    We can use a :class:`Table` to:

    * Check if it :meth:`exists`
    * :meth:`create` itself
    * :meth:`rename` itself
    * :meth:`delete` itself

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

        :rtype: integer
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
        "projects/../zones/../clusters/../tables/{table_id}"

        :rtype: string
        :returns: The table name.
        """
        return self.cluster.name + '/tables/' + self.table_id

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (other.table_id == self.table_id and
                other.cluster == self.cluster)

    def __ne__(self, other):
        return not self.__eq__(other)

    def exists(self, timeout_seconds=None):
        """Check if this table exists.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.

        :rtype: boolean
        :returns: Boolean indicating if this table exists. If it does not
                  exist, an exception will be thrown by the API call.
        """
        request_pb = messages_pb2.GetTableRequest(name=self.name)
        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.GetTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.Table`
            response.result()

        return True

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
                                   [, s1), [s1, s2), [s2, ).

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys or [],
            name=self.cluster.name,
            table_id=self.table_id,
        )
        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.CreateTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.Table`
            response.result()

    def rename(self, new_table_id, timeout_seconds=None):
        """Rename this table.

        .. note::

            This cannot be used to move tables between clusters,
            zones, or projects.

        :type new_table_id: string
        :param new_table_id: The new name table ID.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        request_pb = messages_pb2.RenameTableRequest(
            name=self.name,
            new_id=new_table_id,
        )
        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.RenameTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()

    def delete(self, timeout_seconds=None):
        """Delete this table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        request_pb = messages_pb2.DeleteTableRequest(name=self.name)
        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.DeleteTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()

    def delete_column_family(self, column_family_id, timeout_seconds=None):
        """Delete a column family in this table.

        :type column_family_id: string
        :param column_family_id: The ID of the column family.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on table.
        """
        column_family_name = self.name + '/columnFamilies/' + column_family_id
        request_pb = messages_pb2.DeleteColumnFamilyRequest(
            name=column_family_name)
        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.DeleteColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()
