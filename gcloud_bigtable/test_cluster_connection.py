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


class TestClusterConnection(unittest2.TestCase):

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
        # Let the first Connection() constructor actually require scopes
        # but not the second.
        credentials = _MockWithAttachedMethods(True, new_creds, False)
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is new_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
            (
                'create_scoped',
                ((klass.SCOPE,),),
                {},
            ),
            ('create_scoped_required', (), {}),
        ])

    def test_constructor_bad_type(self):
        from oauth2client.client import AssertionCredentials
        assertion_type = 'ASSERTION_TYPE'
        credentials = AssertionCredentials(assertion_type)
        self.assertRaises(TypeError, self._makeOne, credentials=credentials)

    def _grpc_call_helper(self, call_method, method_name, request_obj):
        from gcloud_bigtable._grpc_mocks import StubMockFactory
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT

        credentials = _MockWithAttachedMethods(False, False)
        connection = self._makeOne(credentials=credentials)

        expected_result = object()
        mock_make_stub = StubMockFactory(expected_result)
        with _Monkey(MUT, make_stub=mock_make_stub):
            result = call_method(connection)

        self.assertTrue(result is expected_result)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
            ('create_scoped_required', (), {}),
        ])

        # Check all the stubs that were created and used as a context
        # manager (should be just one).
        factory_args = (
            credentials,
            MUT.CLUSTER_STUB_FACTORY,
            MUT.CLUSTER_ADMIN_HOST,
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

    def test_get_operation(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT
        # Only need one return value since the OperationsConnection()
        # constructor will not actually be used (a stub instead),
        # hence we only have to deal with one create_scoped_required call.
        credentials = _MockWithAttachedMethods(False)
        # We need to stub out the OperationsConnection since that is
        # what we'll be testing.
        expected_result = object()
        op_conn = _MockWithAttachedMethods(expected_result)
        op_conn_class = _MockCalled(op_conn)
        with _Monkey(MUT, OperationsConnection=op_conn_class):
            connection = self._makeOne(credentials=credentials)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'
        OPERATION_ID = 'OPERATION_ID'
        result = connection.get_operation(PROJECT_ID, ZONE,
                                          CLUSTER_ID, OPERATION_ID)

        # Make sure our mock returned the fake value.
        self.assertTrue(result is expected_result)
        op_conn_class.check_called(
            self,
            [(MUT.CLUSTER_ADMIN_HOST,)],
            [{
                'credentials': credentials,
                'scope': connection.SCOPE,
            }],
        )
        expected_op_name = ('operations/projects/PROJECT_ID/zones/ZONE/'
                            'clusters/CLUSTER_ID/operations/OPERATION_ID')
        self.assertEqual(
            op_conn._called,
            [
                (
                    'get_operation',
                    (expected_op_name,),
                    {'timeout_seconds': MUT.TIMEOUT_SECONDS},
                ),
            ],
        )

    def test_list_zones(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        PROJECT_ID = 'PROJECT_ID'
        request_obj = messages_pb2.ListZonesRequest(
            name='projects/%s' % (PROJECT_ID,),
        )

        def call_method(connection):
            return connection.list_zones(PROJECT_ID)

        self._grpc_call_helper(call_method, 'ListZones', request_obj)

    def test_get_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, ZONE, CLUSTER_ID)
        request_obj = messages_pb2.GetClusterRequest(name=cluster_name)

        def call_method(connection):
            return connection.get_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        self._grpc_call_helper(call_method, 'GetCluster', request_obj)

    def test_list_clusters(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        PROJECT_ID = 'PROJECT_ID'
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

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

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

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

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

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, ZONE, CLUSTER_ID)
        request_obj = messages_pb2.DeleteClusterRequest(name=cluster_name)

        def call_method(connection):
            return connection.delete_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        self._grpc_call_helper(call_method, 'DeleteCluster', request_obj)

    def test_undelete_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

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
        NAME = 'NAME'
        DISPLAY_NAME = 'DISPLAY_NAME'
        SERVE_NODES = 8

        cluster = self._callFUT(name=NAME, display_name=DISPLAY_NAME,
                                serve_nodes=SERVE_NODES)
        all_fields = set(field.name for field in cluster._fields.keys())
        self.assertEqual(all_fields,
                         set(['display_name', 'name', 'serve_nodes']))

        self.assertEqual(cluster.display_name, DISPLAY_NAME)
        self.assertEqual(cluster.serve_nodes, SERVE_NODES)
