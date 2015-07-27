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


PROJECT_ID = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'


class Test__prepare_create_request(unittest2.TestCase):

    def _callFUT(self, cluster):
        from gcloud_bigtable.cluster import _prepare_create_request
        return _prepare_create_request(cluster)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable.cluster import Cluster
        display_name = 'DISPLAY_NAME'
        serve_nodes = 8

        class _Client(object):
            project_id = PROJECT_ID

        cluster = Cluster(ZONE, CLUSTER_ID, _Client,
                          display_name=display_name, serve_nodes=serve_nodes)
        request_pb = self._callFUT(cluster)
        self.assertTrue(isinstance(request_pb,
                                   messages_pb2.CreateClusterRequest))
        self.assertEqual(request_pb.cluster_id, CLUSTER_ID)
        self.assertEqual(request_pb.name,
                         'projects/' + PROJECT_ID + '/zones/' + ZONE)
        self.assertTrue(isinstance(request_pb.cluster, data_pb2.Cluster))
        self.assertEqual(request_pb.cluster.display_name, display_name)
        self.assertEqual(request_pb.cluster.serve_nodes, serve_nodes)


class Test__process_operation(unittest2.TestCase):

    def _callFUT(self, operation_pb):
        from gcloud_bigtable.cluster import _process_operation
        return _process_operation(operation_pb)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        expected_operation_id = 234
        operation_name = ('operations/projects/%s/zones/%s/clusters/%s/'
                          'operations/%d' % (PROJECT_ID, ZONE, CLUSTER_ID,
                                             expected_operation_id))

        current_op = operations_pb2.Operation(name=operation_name)

        request_metadata = messages_pb2.CreateClusterMetadata()
        mock_parse_pb_any_to_native = _MockCalled(request_metadata)
        expected_operation_begin = object()
        mock_pb_timestamp_to_datetime = _MockCalled(expected_operation_begin)
        with _Monkey(MUT, _parse_pb_any_to_native=mock_parse_pb_any_to_native,
                     _pb_timestamp_to_datetime=mock_pb_timestamp_to_datetime):
            operation_id, operation_begin = self._callFUT(current_op)

        self.assertEqual(operation_id, expected_operation_id)
        self.assertTrue(operation_begin is expected_operation_begin)

        mock_parse_pb_any_to_native.check_called(
            self, [(current_op.metadata,)])
        mock_pb_timestamp_to_datetime.check_called(
            self, [(request_metadata.request_time,)])

    def test_failure(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)

        cluster = data_pb2.Cluster()
        with self.assertRaises(ValueError):
            self._callFUT(cluster)


class TestCluster(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import client
        from gcloud_bigtable import cluster as MUT
        cls._MUT = MUT
        cls._STUB_SCOPES = [client.DATA_SCOPE]
        cls._STUB_FACTORY_NAME = 'CLUSTER_STUB_FACTORY'
        cls._STUB_HOST = MUT.CLUSTER_ADMIN_HOST
        cls._STUB_PORT = MUT.CLUSTER_ADMIN_PORT

    @classmethod
    def tearDownClass(cls):
        del cls._MUT
        del cls._STUB_SCOPES
        del cls._STUB_FACTORY_NAME
        del cls._STUB_HOST
        del cls._STUB_PORT

    def _getTargetClass(self):
        from gcloud_bigtable.cluster import Cluster
        return Cluster

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        client = object()
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, CLUSTER_ID)
        self.assertEqual(cluster.serve_nodes, 3)
        self.assertTrue(cluster._client is client)

    def test_constructor_non_default(self):
        client = object()
        display_name = 'display_name'
        serve_nodes = 8
        cluster = self._makeOne(ZONE, CLUSTER_ID, client,
                                display_name=display_name,
                                serve_nodes=serve_nodes)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, display_name)
        self.assertEqual(cluster.serve_nodes, serve_nodes)
        self.assertTrue(cluster._client is client)

    def test_client_getter(self):
        client = object()
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertTrue(cluster.client is client)

    def test_project_id_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=PROJECT_ID)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.project_id, PROJECT_ID)

    def test_name_property(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=PROJECT_ID)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        self.assertEqual(cluster.name, cluster_name)

    def test_from_pb_success(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=PROJECT_ID)

        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster_pb = data_pb2.Cluster(
            name=cluster_name,
            display_name=CLUSTER_ID,
            serve_nodes=3,
        )

        klass = self._getTargetClass()
        cluster = klass.from_pb(cluster_pb, client)
        self.assertTrue(isinstance(cluster, klass))
        self.assertEqual(cluster.client, client)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)

    def test_from_pb_bad_cluster_name(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)

        cluster_name = 'INCORRECT_FORMAT'
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, None)

    def test_from_pb_project_id_mistmatch(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        alt_project_id = 'ALT_PROJECT_ID'
        client = Client(credentials, project_id=alt_project_id)

        self.assertNotEqual(PROJECT_ID, alt_project_id)

        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, client)

    def test___eq__(self):
        zone = 'zone'
        cluster_id = 'cluster_id'
        client = object()
        cluster1 = self._makeOne(zone, cluster_id, client)
        cluster2 = self._makeOne(zone, cluster_id, client)
        self.assertEqual(cluster1, cluster2)

    def test___eq__type_differ(self):
        cluster1 = self._makeOne('zone', 'cluster_id', 'client')
        cluster2 = object()
        self.assertNotEqual(cluster1, cluster2)

    def test___ne__same_value(self):
        zone = 'zone'
        cluster_id = 'cluster_id'
        client = object()
        cluster1 = self._makeOne(zone, cluster_id, client)
        cluster2 = self._makeOne(zone, cluster_id, client)
        comparison_val = (cluster1 != cluster2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        cluster1 = self._makeOne('zone1', 'cluster_id1', 'client1')
        cluster2 = self._makeOne('zone2', 'cluster_id2', 'client2')
        self.assertNotEqual(cluster1, cluster2)

    def test_reload(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        TEST_CASE = self

        # Create request_pb
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.GetClusterRequest(name=cluster_name)

        # Create response_pb
        response_pb = data_pb2.Cluster(
            display_name=CLUSTER_ID,
            serve_nodes=3,
        )
        # Create expected_result.
        expected_result = None  # reload() has no return value.

        # We must create the cluster with the client passed in.
        def result_method(client):
            cluster = TEST_CASE._makeOne(ZONE, CLUSTER_ID, client)
            return cluster.reload()

        self._grpc_client_test_helper('GetCluster', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def test_operation_finished_without_operation(self):
        cluster = self._makeOne(ZONE, CLUSTER_ID, None)
        self.assertEqual(cluster._operation_type, None)
        with self.assertRaises(ValueError):
            cluster.operation_finished()

    def _operation_finished_helper(self, done):
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable import cluster as MUT

        # Create request_pb
        op_id = 789
        op_name = ('operations/projects/' + PROJECT_ID + '/zones/' +
                   ZONE + '/clusters/' + CLUSTER_ID +
                   '/operations/%d' % (op_id,))
        request_pb = operations_pb2.GetOperationRequest(name=op_name)

        # Create response_pb
        response_pb = operations_pb2.Operation(done=done)

        # Create expected_result.
        expected_result = done

        # We must create the cluster with the client passed in.
        TEST_CASE = self
        CLUSTER_CREATED = []
        op_begin = object()
        op_type = object()

        def result_method(client):
            cluster = TEST_CASE._makeOne(ZONE, CLUSTER_ID, client)
            cluster._operation_id = op_id
            cluster._operation_begin = op_begin
            cluster._operation_type = op_type
            CLUSTER_CREATED.append(cluster)
            return cluster.operation_finished()

        self._grpc_client_test_helper('GetOperation', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      stub_factory=MUT.OPERATIONS_STUB_FACTORY)
        self.assertEqual(len(CLUSTER_CREATED), 1)
        if done:
            self.assertEqual(CLUSTER_CREATED[0]._operation_type, None)
            self.assertEqual(CLUSTER_CREATED[0]._operation_id, None)
            self.assertEqual(CLUSTER_CREATED[0]._operation_begin, None)
        else:
            self.assertEqual(CLUSTER_CREATED[0]._operation_type, op_type)
            self.assertEqual(CLUSTER_CREATED[0]._operation_id, op_id)
            self.assertEqual(CLUSTER_CREATED[0]._operation_begin, op_begin)

    def test_operation_finished(self):
        self._operation_finished_helper(done=True)

    def test_operation_finished_not_done(self):
        self._operation_finished_helper(done=False)

    def test_create(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        # Create request_pb. Just a mock since we monkey patch
        # _prepare_create_request
        request_pb = object()

        # Create response_pb
        current_op = operations_pb2.Operation()
        response_pb = data_pb2.Cluster(current_operation=current_op)

        # Create expected_result.
        expected_result = None

        # We must create the cluster with the client passed in.
        TEST_CASE = self
        CLUSTER_CREATED = []

        def result_method(client):
            cluster = TEST_CASE._makeOne(ZONE, CLUSTER_ID, client)
            CLUSTER_CREATED.append(cluster)
            return cluster.create()

        mock_prepare_create_request = _MockCalled(request_pb)
        op_id = 5678
        op_begin = object()
        mock_process_operation = _MockCalled((op_id, op_begin))
        with _Monkey(MUT, _prepare_create_request=mock_prepare_create_request,
                     _process_operation=mock_process_operation):
            self._grpc_client_test_helper('CreateCluster', result_method,
                                          request_pb, response_pb,
                                          expected_result, PROJECT_ID)

        self.assertEqual(len(CLUSTER_CREATED), 1)
        self.assertEqual(CLUSTER_CREATED[0]._operation_type, 'create')
        self.assertEqual(CLUSTER_CREATED[0]._operation_id, op_id)
        self.assertTrue(CLUSTER_CREATED[0]._operation_begin is op_begin)
        mock_prepare_create_request.check_called(self, [(CLUSTER_CREATED[0],)])
        mock_process_operation.check_called(self, [(current_op,)])

    def test_update(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        # Create request_pb
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        serve_nodes = 81
        display_name = 'display_name'
        request_pb = data_pb2.Cluster(
            name=cluster_name,
            display_name=display_name,
            serve_nodes=serve_nodes,
        )

        # Create response_pb
        current_op = operations_pb2.Operation()
        response_pb = data_pb2.Cluster(current_operation=current_op)

        # Create expected_result.
        expected_result = None

        # We must create the cluster object with the client passed in.
        TEST_CASE = self
        CLUSTER_CREATED = []

        def result_method(client):
            cluster = TEST_CASE._makeOne(ZONE, CLUSTER_ID, client,
                                         display_name=display_name,
                                         serve_nodes=serve_nodes)
            CLUSTER_CREATED.append(cluster)
            return cluster.update()

        op_id = 5678
        op_begin = object()
        mock_process_operation = _MockCalled((op_id, op_begin))
        with _Monkey(MUT,
                     _process_operation=mock_process_operation):
            self._grpc_client_test_helper('UpdateCluster', result_method,
                                          request_pb, response_pb,
                                          expected_result, PROJECT_ID)

        self.assertEqual(len(CLUSTER_CREATED), 1)
        self.assertEqual(CLUSTER_CREATED[0]._operation_type, 'update')
        self.assertEqual(CLUSTER_CREATED[0]._operation_id, op_id)
        self.assertTrue(CLUSTER_CREATED[0]._operation_begin is op_begin)
        mock_process_operation.check_called(self, [(current_op,)])

    def test_delete(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2

        TEST_CASE = self

        # Create request_pb
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.DeleteClusterRequest(name=cluster_name)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # We must create the cluster with the client passed in.
        def result_method(client):
            cluster = TEST_CASE._makeOne(ZONE, CLUSTER_ID, client)
            return cluster.delete()

        self._grpc_client_test_helper('DeleteCluster', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def test_undelete(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        # Create request_pb
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.UndeleteClusterRequest(name=cluster_name)

        # Create response_pb
        response_pb = operations_pb2.Operation()

        # Create expected_result.
        expected_result = None

        # We must create the cluster object with the client passed in.
        TEST_CASE = self
        CLUSTER_CREATED = []

        def result_method(client):
            cluster = TEST_CASE._makeOne(ZONE, CLUSTER_ID, client)
            CLUSTER_CREATED.append(cluster)
            return cluster.undelete()

        op_id = 5678
        op_begin = object()
        mock_process_operation = _MockCalled((op_id, op_begin))
        with _Monkey(MUT,
                     _process_operation=mock_process_operation):
            self._grpc_client_test_helper('UndeleteCluster', result_method,
                                          request_pb, response_pb,
                                          expected_result, PROJECT_ID)

        self.assertEqual(len(CLUSTER_CREATED), 1)
        self.assertEqual(CLUSTER_CREATED[0]._operation_type, 'undelete')
        self.assertEqual(CLUSTER_CREATED[0]._operation_id, op_id)
        self.assertTrue(CLUSTER_CREATED[0]._operation_begin is op_begin)
        mock_process_operation.check_called(self, [(response_pb,)])
