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


PROJECT_ID = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'


class TestCluster(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import client
        from gcloud_bigtable import cluster_standalone as MUT
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
        from gcloud_bigtable.cluster_standalone import Cluster
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
