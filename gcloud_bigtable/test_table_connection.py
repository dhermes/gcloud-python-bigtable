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
COLUMN_FAMILY_ID = 'COLUMN_FAMILY_ID'


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

    def test_update_column_family(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        credentials = _MockWithAttachedMethods(False)
        connection = self._makeOne(credentials=credentials)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])

        self.assertRaises(NotImplementedError, connection.update_column_family,
                          CLUSTER_NAME, TABLE_ID, COLUMN_FAMILY_ID)

    def test_delete_column_family(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        column_family_name = '%s/tables/%s/columnFamilies/%s' % (
            CLUSTER_NAME, TABLE_ID, COLUMN_FAMILY_ID)
        request_obj = messages_pb2.DeleteColumnFamilyRequest(
            name=column_family_name)

        def call_method(connection):
            return connection.delete_column_family(
                CLUSTER_NAME, TABLE_ID, COLUMN_FAMILY_ID)

        self._grpc_call_helper(call_method, 'DeleteColumnFamily', request_obj)
