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


class TestOperationsConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.operations_connection import OperationsConnection
        return OperationsConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))

    def test_get_operation(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertRaises(NotImplementedError, connection.get_operation)

    def test_list_operations(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertRaises(NotImplementedError, connection.list_operations)

    def test_cancel_operation(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertRaises(NotImplementedError, connection.cancel_operation)

    def test_delete_operation(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertRaises(NotImplementedError, connection.delete_operation)
