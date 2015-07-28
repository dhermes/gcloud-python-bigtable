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

    def test_name_property(self):
        cluster_name = 'cluster_name'
        cluster = _Cluster(cluster_name)
        table = self._makeOne(TABLE_ID, cluster)
        expected_name = cluster_name + '/tables/' + TABLE_ID
        self.assertEqual(table.name, expected_name)

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

    def test_exists(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.GetTableRequest(name=table_name)

        # Create response_pb
        response_pb = None

        # Create expected_result.
        expected_result = True

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.exists()

        self._grpc_client_test_helper('GetTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

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

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.delete()

        self._grpc_client_test_helper('DeleteTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)


class _Cluster(object):

    def __init__(self, name):
        self.name = name
