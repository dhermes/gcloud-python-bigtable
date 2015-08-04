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


import unittest2


PROJECT_ID = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'
TABLE_ID = 'table-id'


class TestTable(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.table import Table
        return Table

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        cluster = object()
        table = self._makeOne(TABLE_ID, cluster)
        self.assertEqual(table.table_id, TABLE_ID)
        self.assertTrue(table._cluster is cluster)

    def test_cluster_getter(self):
        cluster = object()
        table = self._makeOne(TABLE_ID, cluster)
        self.assertTrue(table.cluster is cluster)

    def test_client_getter(self):
        client = object()
        cluster = _Cluster(None, client=client)
        table = self._makeOne(TABLE_ID, cluster)
        self.assertTrue(table.client is client)

    def test_timeout_seconds_getter(self):
        timeout_seconds = 1001
        cluster = _Cluster(None, timeout_seconds=timeout_seconds)
        table = self._makeOne(TABLE_ID, cluster)
        self.assertEqual(table.timeout_seconds, timeout_seconds)

    def test_name_property(self):
        cluster_name = 'cluster_name'
        cluster = _Cluster(cluster_name)
        table = self._makeOne(TABLE_ID, cluster)
        expected_name = cluster_name + '/tables/' + TABLE_ID
        self.assertEqual(table.name, expected_name)

    def test_column_family_factory(self):
        from gcloud_bigtable.column_family import ColumnFamily

        table = self._makeOne(TABLE_ID, None)
        gc_rule = object()
        column_family_id = 'column_family_id'
        column_family = table.column_family(column_family_id, gc_rule=gc_rule)
        self.assertTrue(isinstance(column_family, ColumnFamily))
        self.assertEqual(column_family.column_family_id, column_family_id)
        self.assertTrue(column_family.gc_rule is gc_rule)
        self.assertEqual(column_family._table, table)

    def test_row_factory(self):
        from gcloud_bigtable.row import Row

        table = self._makeOne(TABLE_ID, None)
        row_key = 'row_key'
        row = table.row(row_key)
        self.assertTrue(isinstance(row, Row))
        self.assertEqual(row.row_key, row_key)
        self.assertEqual(row._table, table)

    def test___eq__(self):
        table_id = 'table_id'
        cluster = object()
        table1 = self._makeOne(table_id, cluster)
        table2 = self._makeOne(table_id, cluster)
        self.assertEqual(table1, table2)

    def test___eq__type_differ(self):
        table1 = self._makeOne('table_id', None)
        table2 = object()
        self.assertNotEqual(table1, table2)

    def test___ne__same_value(self):
        table_id = 'table_id'
        cluster = object()
        table1 = self._makeOne(table_id, cluster)
        table2 = self._makeOne(table_id, cluster)
        comparison_val = (table1 != table2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        table1 = self._makeOne('table_id1', 'cluster1')
        table2 = self._makeOne('table_id2', 'cluster2')
        self.assertNotEqual(table1, table2)

    def _create_test_helper(self, initial_split_keys):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client()
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster = _Cluster(cluster_name, client=client)
        table = self._makeOne(TABLE_ID, cluster)

        # Create request_pb
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys,
            name=cluster_name,
            table_id=TABLE_ID,
        )

        # Create response_pb
        response_pb = data_pb2.Table()

        # Patch the stub used by the API method.
        client.table_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = None  # create() has no return value.

        # Perform the method and check the result.
        timeout_seconds = 150
        result = table.create(initial_split_keys=initial_split_keys,
                              timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'CreateTable',
            (request_pb, timeout_seconds),
            {},
        )])

    def test_create(self):
        initial_split_keys = None
        self._create_test_helper(initial_split_keys)

    def test_create_with_split_keys(self):
        initial_split_keys = ['s1', 's2']
        self._create_test_helper(initial_split_keys)

    def test_rename(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2
        from gcloud_bigtable._grpc_mocks import StubMock

        new_table_id = 'new_table_id'
        self.assertNotEqual(new_table_id, TABLE_ID)

        client = _Client()
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster = _Cluster(cluster_name, client=client)
        table = self._makeOne(TABLE_ID, cluster)

        # Create request_pb
        table_name = cluster_name + '/tables/' + TABLE_ID
        request_pb = messages_pb2.RenameTableRequest(
            name=table_name,
            new_id=new_table_id,
        )

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        client.table_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = None  # rename() has no return value.

        # Perform the method and check the result.
        timeout_seconds = 97
        result = table.rename(new_table_id, timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'RenameTable',
            (request_pb, timeout_seconds),
            {},
        )])

    def test_delete(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client()
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster = _Cluster(cluster_name, client=client)
        table = self._makeOne(TABLE_ID, cluster)

        # Create request_pb
        table_name = cluster_name + '/tables/' + TABLE_ID
        request_pb = messages_pb2.DeleteTableRequest(name=table_name)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        client.table_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # Perform the method and check the result.
        timeout_seconds = 871
        result = table.delete(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'DeleteTable',
            (request_pb, timeout_seconds),
            {},
        )])

    def _list_column_families_helper(self, column_family_name=None):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable.column_family import ColumnFamily

        client = _Client()
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster = _Cluster(cluster_name, client=client)
        table = self._makeOne(TABLE_ID, cluster)

        # Create request_pb
        table_name = cluster_name + '/tables/' + TABLE_ID
        request_pb = messages_pb2.GetTableRequest(name=table_name)

        # Create response_pb
        column_family_id = 'foo'
        if column_family_name is None:
            column_family_name = (table_name + '/columnFamilies/' +
                                  column_family_id)
        column_family = data_pb2.ColumnFamily(name=column_family_name)
        response_pb = data_pb2.Table(
            column_families={column_family_id: column_family},
        )

        # Patch the stub used by the API method.
        client.table_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = {
            column_family_id: ColumnFamily(column_family_id, table),
        }

        # Perform the method and check the result.
        timeout_seconds = 502
        result = table.list_column_families(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'GetTable',
            (request_pb, timeout_seconds),
            {},
        )])

    def test_list_column_families(self):
        self._list_column_families_helper()

    def test_list_column_families_failure(self):
        column_family_name = 'not-the-right-format'
        with self.assertRaises(ValueError):
            self._list_column_families_helper(
                column_family_name=column_family_name)


class _Client(object):

    cluster_stub = None
    operations_stub = None
    table_stub = None


class _Cluster(object):

    def __init__(self, name, client=None, timeout_seconds=None):
        self.name = name
        self.client = client
        self.timeout_seconds = timeout_seconds
