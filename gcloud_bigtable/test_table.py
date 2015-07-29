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


from gcloud_bigtable._grpc_mocks import GRPCMockTestMixin


PROJECT_ID = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'
TABLE_ID = 'table-id'


class TestTable(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import client
        from gcloud_bigtable import table as MUT
        cls._MUT = MUT
        cls._STUB_SCOPES = [client.DATA_SCOPE]
        cls._STUB_FACTORY_NAME = 'TABLE_STUB_FACTORY'
        cls._STUB_HOST = MUT.TABLE_ADMIN_HOST
        cls._STUB_PORT = MUT.TABLE_ADMIN_PORT

    @classmethod
    def tearDownClass(cls):
        del cls._MUT
        del cls._STUB_SCOPES
        del cls._STUB_FACTORY_NAME
        del cls._STUB_HOST
        del cls._STUB_PORT

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

        # Create request_pb
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys,
            name=cluster_name,
            table_id=TABLE_ID,
        )

        # Create response_pb
        response_pb = data_pb2.Table()

        # Create expected_result.
        expected_result = None  # create() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self
        timeout_seconds = 150

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.create(initial_split_keys=initial_split_keys,
                                timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('CreateTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)

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

        new_table_id = 'new_table_id'
        self.assertNotEqual(new_table_id, TABLE_ID)

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.RenameTableRequest(
            name=table_name,
            new_id=new_table_id,
        )

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Create expected_result.
        expected_result = None  # rename() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self
        timeout_seconds = 97

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.rename(new_table_id, timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('RenameTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)

    def test_delete(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.DeleteTableRequest(name=table_name)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self
        timeout_seconds = 871

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.delete(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('DeleteTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)

    def test_list_column_families(self):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable.column_family import ColumnFamily

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.GetTableRequest(name=table_name)

        # Create response_pb
        column_family_id = 'foo'
        column_family_name = table_name + '/columnFamilies/' + column_family_id
        column_family = data_pb2.ColumnFamily(name=column_family_name)
        response_pb = data_pb2.Table(
            column_families={column_family_id: column_family},
        )

        # Create expected_result.
        expected_result = {}  # Will update this once test table is created.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self
        timeout_seconds = 502

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            expected_result[column_family_id] = ColumnFamily(
                column_family_id, table)
            return table.list_column_families(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('GetTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)


class _Cluster(object):

    def __init__(self, name, client=None, timeout_seconds=None):
        self.name = name
        self.client = client
        self.timeout_seconds = timeout_seconds
