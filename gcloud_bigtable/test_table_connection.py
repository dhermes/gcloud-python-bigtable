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


PROJECT_ID = 'PROJECT_ID'
ZONE = 'ZONE'
CLUSTER_ID = 'CLUSTER_ID'
CLUSTER_NAME = 'projects/%s/zones/%s/clusters/%s' % (
    PROJECT_ID, ZONE, CLUSTER_ID)
TABLE_ID = 'TABLE_ID'


class TestTableConnection(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import table_connection as MUT
        cls._MUT = MUT
        cls._STUB_FACTORY_NAME = 'TABLE_STUB_FACTORY'
        cls._STUB_HOST = MUT.TABLE_ADMIN_HOST
        cls._STUB_PORT = MUT.PORT

    @classmethod
    def tearDownClass(cls):
        del cls._MUT
        del cls._STUB_FACTORY_NAME
        del cls._STUB_HOST
        del cls._STUB_PORT

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.table_connection import TableConnection
        return TableConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        klass = self._getTargetClass()
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(True, scoped_creds)
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is scoped_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
            ('create_scoped', ((klass.SCOPE,),), {}),
        ])

    def _create_table_test_helper(self, initial_split_keys=None):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        request_obj = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys,
            name=CLUSTER_NAME,
            table_id=TABLE_ID,
        )

        def call_method(connection):
            return connection.create_table(
                CLUSTER_NAME, TABLE_ID, initial_split_keys=initial_split_keys)

        self._grpc_call_helper(call_method, 'CreateTable', request_obj)

    def test_create_table(self):
        initial_split_keys = ['foo', 'bar']
        self._create_table_test_helper(initial_split_keys=initial_split_keys)

    def test_create_table_without_split_keys(self):
        self._create_table_test_helper()

    def test_list_tables(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        request_obj = messages_pb2.ListTablesRequest(name=CLUSTER_NAME)

        def call_method(connection):
            return connection.list_tables(CLUSTER_NAME)

        self._grpc_call_helper(call_method, 'ListTables', request_obj)

    def test_get_table(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        table_name = '%s/tables/%s' % (CLUSTER_NAME, TABLE_ID)
        request_obj = messages_pb2.GetTableRequest(name=table_name)

        def call_method(connection):
            return connection.get_table(CLUSTER_NAME, TABLE_ID)

        self._grpc_call_helper(call_method, 'GetTable', request_obj)

    def test_delete_table(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        table_name = '%s/tables/%s' % (CLUSTER_NAME, TABLE_ID)
        request_obj = messages_pb2.DeleteTableRequest(name=table_name)

        def call_method(connection):
            return connection.delete_table(CLUSTER_NAME, TABLE_ID)

        self._grpc_call_helper(call_method, 'DeleteTable', request_obj)

    def test_rename_table(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        table_name = '%s/tables/%s' % (CLUSTER_NAME, TABLE_ID)
        new_table_id = 'NEW_TABLE_ID'
        request_obj = messages_pb2.RenameTableRequest(
            name=table_name,
            new_id=new_table_id,
        )

        def call_method(connection):
            return connection.rename_table(CLUSTER_NAME, TABLE_ID,
                                           new_table_id)

        self._grpc_call_helper(call_method, 'RenameTable', request_obj)

    def test_create_column_family(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        table_name = '%s/tables/%s' % (CLUSTER_NAME, TABLE_ID)
        column_family_id = 'COLUMN_FAMILY_ID'
        request_obj = messages_pb2.CreateColumnFamilyRequest(
            column_family_id=column_family_id,
            name=table_name,
        )

        def call_method(connection):
            return connection.create_column_family(
                CLUSTER_NAME, TABLE_ID, column_family_id)

        self._grpc_call_helper(call_method, 'CreateColumnFamily', request_obj)

    def test_update_column_family(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        column_family = 'column_family'
        self.assertRaises(NotImplementedError, connection.update_column_family,
                          cluster_name, table_name, column_family)

    def test_delete_column_family(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        column_family = 'column_family'
        self.assertRaises(NotImplementedError, connection.delete_column_family,
                          cluster_name, table_name, column_family)
