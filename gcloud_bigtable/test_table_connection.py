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


class Test_make_table_stub(unittest2.TestCase):

    def _callFUT(self, credentials):
        from gcloud_bigtable.table_connection import make_table_stub
        return make_table_stub(credentials)

    def test_it(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import table_connection as MUT

        mock_result = object()
        custom_factory = _MockCalled(mock_result)
        transformed = object()
        transformer = _MockCalled(transformed)

        certs = 'FOOBAR'
        credentials = _MockWithAttachedMethods()
        with _Monkey(MUT, TABLE_STUB_FACTORY=custom_factory,
                     get_certs=lambda: certs,
                     MetadataTransformer=transformer):
            result = self._callFUT(credentials)

        self.assertTrue(result is mock_result)
        custom_factory.check_called(
            self,
            [(MUT.TABLE_ADMIN_HOST, MUT.PORT)],
            [{
                'metadata_transformer': transformed,
                'secure': True,
                'root_certificates': certs,
            }],
        )
        transformer.check_called(self, [(credentials,)])
        self.assertEqual(credentials._called, [])


class TestTableConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.table_connection import TableConnection
        return TableConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))

    def test_create_table(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        self.assertRaises(NotImplementedError, connection.create_table,
                          cluster_name)

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
