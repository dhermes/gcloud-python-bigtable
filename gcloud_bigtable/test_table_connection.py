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

    def test_build_api_url(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        api_url = klass.build_api_url(cluster_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            cluster_name,
            'tables',
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_table_method_missing_table_name(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        table_method = 'table_method'
        self.assertRaises(ValueError, klass.build_api_url, cluster_name,
                          table_method=table_method)

    def test_build_api_url_table_method_conflict(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        table_method = 'table_method'
        table_name = 'table_name'
        column_family = 'column_family'
        self.assertRaises(ValueError, klass.build_api_url, cluster_name,
                          table_method=table_method,
                          table_name=table_name, column_family=column_family)

    def test_build_api_url_table_method_success(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        table_method = 'table_method'
        table_name = 'table_name'
        api_url = klass.build_api_url(cluster_name, table_method=table_method,
                                      table_name=table_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            cluster_name,
            'tables',
            table_name + table_method,
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_column_family_missing_table_name(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        column_family = 'column_family'
        self.assertRaises(ValueError, klass.build_api_url, cluster_name,
                          column_family=column_family)

    def test_build_api_url_column_family_success(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        column_family = 'column_family'
        table_name = 'table_name'
        api_url = klass.build_api_url(
            cluster_name, column_family=column_family, table_name=table_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            cluster_name,
            'tables',
            table_name,
            'columnFamilies',
            column_family,
        ])
        self.assertEqual(api_url, expected_url)

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
