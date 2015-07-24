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

"""Connection to Google Cloud Bigtable Table Admin API."""


from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._generated import bigtable_table_service_pb2
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import TIMEOUT_SECONDS
from gcloud_bigtable.connection import make_stub


TABLE_STUB_FACTORY = (bigtable_table_service_pb2.
                      early_adopter_create_BigtableTableService_stub)
TABLE_ADMIN_HOST = 'bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""
PORT = 443


class TableConnection(Connection):
    """Connection to Google Cloud Bigtable Table API.

    This only allows interacting with tables in a cluster.

    The ``cluster_name`` value must take the form:
        "projects/*/zones/*/clusters/*"
    """

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
    """Scope for Table Admin and Cluster Admin API requests."""

    def create_table(self, cluster_name, table_id, initial_split_keys=None,
                     timeout_seconds=TIMEOUT_SECONDS):
        """Create a table in an existing cluster.

        .. note::
          Though a :class:`._generated.bigtable_table_data_pb2.Table` is also
          allowed (as the ``table`` property) in a create table request, we do
          not support it in this method. This is because (as the proto says)
          in a request:

            "The `name` field of the Table and all of its ColumnFamilies must
             be left blank, and will be populated in the response."

          This leaves only the ``current_operation`` and ``granularity``
          fields. The ``current_operation`` is only used for responses while
          ``granularity`` is an enum with only one value.

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the table will be
                             created. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type table_id: string
        :param table_id: The name of the table within the cluster.

        :type initial_split_keys: interable of strings
        :param initial_split_keys: (Optional) List of row keys that will be
                                   used to initially split the table into
                                   several tablets (Tablets are similar to H
                                   Base regions). Given two split keys, "s1"
                                   and "s2", three tablets will be created,
                                   spanning the key ranges:
                                   [, s1), [s1, s2), [s2, ).

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`._generated.bigtable_table_data_pb2.Table`
        :returns: The table created.
        """
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys or [],
            name=cluster_name,
            table_id=table_id,
        )
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.CreateTable.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def list_tables(self, cluster_name, timeout_seconds=TIMEOUT_SECONDS):
        """List tables in an existing cluster.

        :type cluster_name: string
        :param cluster_name: The name of the cluster containing the list of
                             tables. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`messages_pb2.ListTablesResponse`
        :returns: The list of tables retrieved.
        """
        request_pb = messages_pb2.ListTablesRequest(name=cluster_name)
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.ListTables.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def get_table(self, cluster_name, table_id,
                  timeout_seconds=TIMEOUT_SECONDS):
        """Gets table metadata.

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the table will be
                             created. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type table_id: string
        :param table_id: The name of the table within the cluster.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`._generated.bigtable_table_data_pb2.Table`
        :returns: The response object for the get table request.
        """
        table_name = '%s/tables/%s' % (cluster_name, table_id)
        request_pb = messages_pb2.GetTableRequest(name=table_name)
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.GetTable.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def delete_table(self, cluster_name, table_id,
                     timeout_seconds=TIMEOUT_SECONDS):
        """Deletes a table.

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the table will be
                             deleted. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type table_id: string
        :param table_id: The name of the table within the cluster.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`gcloud_bigtable._generated.empty_pb2.Empty`
        :returns: The empty response object for the delete table request.
        """
        table_name = '%s/tables/%s' % (cluster_name, table_id)
        request_pb = messages_pb2.DeleteTableRequest(name=table_name)
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.DeleteTable.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def rename_table(self, cluster_name, old_table_id,
                     new_table_id, timeout_seconds=TIMEOUT_SECONDS):
        """Rename a table.

        .. note::
          This cannot be used to move tables between clusters,
          zones, or projects.

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the table will be
                             renamed. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type old_table_id: string
        :param old_table_id: The name of the table being renamed.

        :type new_table_id: string
        :param new_table_id: The new name of the table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`gcloud_bigtable._generated.empty_pb2.Empty`
        :returns: The empty response object for the rename table request.
        """
        curr_table_name = '%s/tables/%s' % (cluster_name, old_table_id)
        request_pb = messages_pb2.RenameTableRequest(
            name=curr_table_name,
            new_id=new_table_id,
        )
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.RenameTable.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def create_column_family(self, cluster_name, table_id, column_family_id,
                             timeout_seconds=TIMEOUT_SECONDS):
        """Create a column family in a table.

        .. note::
          For now, we do not support passing in a custom column family in the
          request. Aside from the ``column_family_id``, the fields being left
          out are

          * ``gc_expression``
          * ``gc_rule``

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the column family
                             will be created. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type table_id: string
        :param table_id: The name of the table within the cluster.

        :type column_family_id: string
        :param column_family_id: The name of the column family within
                                 the table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`._generated.bigtable_table_data_pb2.ColumnFamily`
        :returns: The column family created.
        """
        table_name = '%s/tables/%s' % (cluster_name, table_id)
        request_pb = messages_pb2.CreateColumnFamilyRequest(
            column_family_id=column_family_id,
            name=table_name,
        )
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.CreateColumnFamily.async(request_pb,
                                                     timeout_seconds)
            result_pb = response.result()

        return result_pb

    def update_column_family(self, cluster_name, table_id, column_family_id,
                             timeout_seconds=TIMEOUT_SECONDS):
        """Update an existing column family in a table.

        .. note::
          As in :meth:`create_column_family`, we don't currently support
          setting

          * ``gc_expression``
          * ``gc_rule``

          As a result, there is nothing else to be changed in this method.

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the column family
                             will be updated. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type table_id: string
        :param table_id: The name of the table within the cluster.

        :type column_family_id: string
        :param column_family_id: The name of the column family within
                                 the table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :raises: :class:`NotImplementedError`
        """
        raise NotImplementedError('UpdateColumnFamily is not currently '
                                  'supported due to lack of support for '
                                  'garbage colection settings.')

    def delete_column_family(self, cluster_name, table_id, column_family_id,
                             timeout_seconds=TIMEOUT_SECONDS):
        """Delete an existing column family in a table.

        .. note::
          This permanently deletes a specified column family and all of
          its data.

        :type cluster_name: string
        :param cluster_name: The name of the cluster where the column family
                             will be deleted. Must be of the form
                                 "projects/*/zones/*/clusters/*"
                             Since this is a low-level class, we don't check
                             this, rather we expect callers to pass correctly
                             formatted data.

        :type table_id: string
        :param table_id: The name of the table within the cluster.

        :type column_family_id: string
        :param column_family_id: The name of the column family within
                                 the table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`gcloud_bigtable._generated.empty_pb2.Empty`
        :returns: The empty response object for the delete column
                  family request.
        """
        column_family_name = '%s/tables/%s/columnFamilies/%s' % (
            cluster_name, table_id, column_family_id)
        request_pb = messages_pb2.DeleteColumnFamilyRequest(
            name=column_family_name)
        result_pb = None
        stub = make_stub(self._credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, PORT)
        with stub:
            response = stub.DeleteColumnFamily.async(request_pb,
                                                     timeout_seconds)
            result_pb = response.result()

        return result_pb
