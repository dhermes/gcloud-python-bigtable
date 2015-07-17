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


class TestDataConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.data_connection import DataConnection
        return DataConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))
        self.assertTrue(connection._http is None)

    def test_build_api_url(self):
        klass = self._getTargetClass()
        table_name = 'table_name'
        rpc_method = 'rpc_method'
        api_url = klass.build_api_url(table_name, rpc_method)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            table_name,
            'rows:' + rpc_method,
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_with_row_key(self):
        klass = self._getTargetClass()
        table_name = 'table_name'
        rpc_method = 'rpc_method'
        row_key = 'row_key'
        api_url = klass.build_api_url(table_name, rpc_method, row_key=row_key)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            table_name,
            'rows',
            row_key + ':' + rpc_method,
        ])
        self.assertEqual(api_url, expected_url)

    def test_read_rows(self):
        self.assertTrue(False)

    def test_sample_row_keys(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        self.assertRaises(NotImplementedError, connection.sample_row_keys,
                          table_name)

    def test_mutate_row(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError, connection.mutate_row,
                          table_name, row_key)

    def test_check_and_mutate_row(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError, connection.check_and_mutate_row,
                          table_name, row_key)

    def test_read_modify_write_row(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError,
                          connection.read_modify_write_row,
                          table_name, row_key)
