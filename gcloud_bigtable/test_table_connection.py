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


PROJECT_ID = 'PROJECT_ID'
ZONE = 'ZONE'
CLUSTER_ID = 'CLUSTER_ID'
CLUSTER_NAME = 'projects/%s/zones/%s/clusters/%s' % (
    PROJECT_ID, ZONE, CLUSTER_ID)
TABLE_ID = 'TABLE_ID'


class TestTableConnection(unittest2.TestCase):

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

    def _grpc_call_helper(self, call_method, method_name, request_obj,
                          stub_factory=None):
        from gcloud_bigtable._grpc_mocks import StubMockFactory
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import table_connection as MUT

        credentials = _MockWithAttachedMethods(False)
        connection = self._makeOne(credentials=credentials)

        expected_result = object()
        mock_make_stub = StubMockFactory(expected_result)
        with _Monkey(MUT, make_stub=mock_make_stub):
            result = call_method(connection)

        self.assertTrue(result is expected_result)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])

        # Check all the stubs that were created and used as a context
        # manager (should be just one).
        factory_args = (
            credentials,
            stub_factory or MUT.TABLE_STUB_FACTORY,
            MUT.TABLE_ADMIN_HOST,
            MUT.PORT,
        )
        self.assertEqual(mock_make_stub.factory_calls,
                         [(factory_args, {})])
        stub, = mock_make_stub.stubs  # Asserts just one.
        self.assertEqual(stub._enter_calls, 1)
        self.assertEqual(stub._exit_args,
                         [(None, None, None)])
        # Check all the method calls.
        method_calls = [
            (
                method_name,
                (request_obj, MUT.TIMEOUT_SECONDS),
                {},
            )
        ]
        self.assertEqual(mock_make_stub.method_calls, method_calls)

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
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        self.assertRaises(NotImplementedError, connection.list_tables,
                          cluster_name)

    def test_get_table(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.get_table,
                          cluster_name, table_name)

    def test_delete_table(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.delete_table,
                          cluster_name, table_name)

    def test_rename_table(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.rename_table,
                          cluster_name, table_name)

    def test_create_column_family(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.create_column_family,
                          cluster_name, table_name)

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
