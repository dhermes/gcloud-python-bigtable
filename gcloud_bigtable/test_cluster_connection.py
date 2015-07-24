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

from gcloud_bigtable._grpc_mocks import GRPCMockTestMixin


PROJECT_ID = 'PROJECT_ID'
ZONE = 'ZONE'
CLUSTER_ID = 'CLUSTER_ID'


class TestClusterConnection(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import cluster_connection as MUT
        cls._MUT = MUT
        cls._STUB_FACTORY_NAME = 'CLUSTER_STUB_FACTORY'
        cls._STUB_HOST = MUT.CLUSTER_ADMIN_HOST
        cls._STUB_PORT = MUT.PORT

    @classmethod
    def tearDownClass(cls):
        del cls._MUT
        del cls._STUB_FACTORY_NAME
        del cls._STUB_HOST
        del cls._STUB_PORT

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.cluster_connection import ClusterConnection
        return ClusterConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        klass = self._getTargetClass()
        new_creds = object()
        credentials = _MockWithAttachedMethods(True, new_creds)
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is new_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
            (
                'create_scoped',
                ((klass.SCOPE,),),
                {},
            ),
        ])

    def test_constructor_bad_type(self):
        from oauth2client.client import AssertionCredentials
        assertion_type = 'ASSERTION_TYPE'
        credentials = AssertionCredentials(assertion_type)
        self.assertRaises(TypeError, self._makeOne, credentials=credentials)

    def test_get_operation(self):
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable import cluster_connection as MUT

        operation_id = 'OPERATION_ID'
        operation_name = (
            'operations/projects/%s/zones/%s/clusters/%s/operations/%s' % (
                PROJECT_ID, ZONE, CLUSTER_ID, operation_id))
        request_obj = operations_pb2.GetOperationRequest(name=operation_name)

        def call_method(connection):
            return connection.get_operation(PROJECT_ID, ZONE,
                                            CLUSTER_ID, operation_id)

        self._grpc_call_helper(call_method, 'GetOperation', request_obj,
                               stub_factory=MUT.OPERATIONS_STUB_FACTORY)

    def test_list_zones(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        request_obj = messages_pb2.ListZonesRequest(
            name='projects/%s' % (PROJECT_ID,),
        )

        def call_method(connection):
            return connection.list_zones(PROJECT_ID)

        self._grpc_call_helper(call_method, 'ListZones', request_obj)

    def test_get_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, ZONE, CLUSTER_ID)
        request_obj = messages_pb2.GetClusterRequest(name=cluster_name)

        def call_method(connection):
            return connection.get_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        self._grpc_call_helper(call_method, 'GetCluster', request_obj)

    def test_list_clusters(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        request_obj = messages_pb2.ListClustersRequest(
            name='projects/%s' % (PROJECT_ID,),
        )

        def call_method(connection):
            return connection.list_clusters(PROJECT_ID)

        self._grpc_call_helper(call_method, 'ListClusters', request_obj)

    def test_create_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT

        def call_method(connection):
            return connection.create_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        cluster_obj = data_pb2.Cluster()
        mock_prepare_cluster = _MockCalled(cluster_obj)
        request_obj = messages_pb2.CreateClusterRequest(
            name='projects/%s/zones/%s' % (PROJECT_ID, ZONE),
            cluster_id=CLUSTER_ID,
            cluster=cluster_obj,
        )

        with _Monkey(MUT, _prepare_cluster=mock_prepare_cluster):
            self._grpc_call_helper(call_method, 'CreateCluster', request_obj)

        mock_prepare_cluster.check_called(
            self,
            [()],  # No positional args.
            [{'display_name': None, 'serve_nodes': None}],
        )

    def test_update_cluster(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT

        def call_method(connection):
            return connection.update_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        request_obj = object()
        mock_prepare_cluster = _MockCalled(request_obj)
        with _Monkey(MUT, _prepare_cluster=mock_prepare_cluster):
            self._grpc_call_helper(call_method, 'UpdateCluster', request_obj)

        project_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, ZONE, CLUSTER_ID)
        mock_prepare_cluster.check_called(
            self,
            [()],  # No positional args.
            [{
                'display_name': None,
                'name': project_name,
                'serve_nodes': None,
            }],
        )

    def test_delete_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, ZONE, CLUSTER_ID)
        request_obj = messages_pb2.DeleteClusterRequest(name=cluster_name)

        def call_method(connection):
            return connection.delete_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        self._grpc_call_helper(call_method, 'DeleteCluster', request_obj)

    def test_undelete_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, ZONE, CLUSTER_ID)
        request_obj = messages_pb2.UndeleteClusterRequest(name=cluster_name)

        def call_method(connection):
            return connection.undelete_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        self._grpc_call_helper(call_method, 'UndeleteCluster', request_obj)


class Test__prepare_cluster(unittest2.TestCase):

    def _callFUT(self, name=None, display_name=None, serve_nodes=None):
        from gcloud_bigtable.cluster_connection import _prepare_cluster
        return _prepare_cluster(name=name, display_name=display_name,
                                serve_nodes=serve_nodes)

    def test_defaults(self):
        cluster = self._callFUT()
        all_fields = set(field.name for field in cluster._fields.keys())
        self.assertEqual(all_fields, set())

    def test_non_default_arguments(self):
        name = 'NAME'
        display_name = 'DISPLAY_NAME'
        serve_nodes = 8

        cluster = self._callFUT(name=name, display_name=display_name,
                                serve_nodes=serve_nodes)
        all_fields = set(field.name for field in cluster._fields.keys())
        self.assertEqual(all_fields,
                         set(['display_name', 'name', 'serve_nodes']))

        self.assertEqual(cluster.display_name, display_name)
        self.assertEqual(cluster.serve_nodes, serve_nodes)
