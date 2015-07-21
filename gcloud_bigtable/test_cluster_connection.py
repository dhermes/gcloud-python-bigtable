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

from gcloud_bigtable._testing import _StubMock


class Test_make_cluster_stub(unittest2.TestCase):

    def _callFUT(self, credentials):
        from gcloud_bigtable.cluster_connection import make_cluster_stub
        return make_cluster_stub(credentials)

    def test_it(self):
        from gcloud_bigtable._testing import _Credentials
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT

        called = []
        creds_list = []
        mock_result = object()
        transformed = object()

        def custom_factory(*args, **kwargs):
            called.append((args, kwargs))
            return mock_result

        def transformer(credentials):
            creds_list.append(credentials)
            return transformed

        certs = 'FOOBAR'
        credentials = _Credentials()
        with _Monkey(MUT, CLUSTER_STUB_FACTORY=custom_factory,
                     get_certs=lambda: certs,
                     MetadataTransformer=transformer):
            result = self._callFUT(credentials)

        self.assertTrue(result is mock_result)
        self.assertEqual(creds_list, [credentials])
        # Unpack single call.
        (called_args, called_kwargs), = called
        self.assertEqual(called_args,
                         (MUT.CLUSTER_ADMIN_HOST, MUT.PORT))
        expected_kwargs = {
            'metadata_transformer': transformed,
            'secure': True,
            'root_certificates': certs,
        }
        self.assertEqual(called_kwargs, expected_kwargs)


class TestClusterConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.cluster_connection import ClusterConnection
        return ClusterConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))

    def test_constructor_bad_type(self):
        from oauth2client.client import AssertionCredentials
        assertion_type = 'ASSERTION_TYPE'
        credentials = AssertionCredentials(assertion_type)
        self.assertRaises(TypeError, self._makeOne, credentials=credentials)

    def _rpc_method_test_helper(self, rpc_method, method_name):
        from gcloud_bigtable._testing import _Credentials
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        stubs = []
        expected_result = object()

        def mock_make_stub(creds):
            stub = _ClusterStubMock(creds, expected_result, method_name)
            stubs.append(stub)
            return stub

        with _Monkey(MUT, make_cluster_stub=mock_make_stub):
            result = rpc_method(connection)

        self.assertTrue(result is expected_result)
        return credentials, stubs

    def _check_rpc_stubs_used(self, credentials, stubs, request_type):
        # Asserting length 1 by unpacking.
        stub_used, = stubs
        self.assertTrue(stub_used._credentials is credentials)
        self.assertEqual(stub_used._enter_calls, 1)

        # Asserting length 1 (and a 3-tuple) by unpacking.
        (exc_type, exc_val, _), = stub_used._exit_args
        self.assertTrue(exc_type is None)
        self.assertTrue(isinstance(exc_val, type(None)))

        # Asserting length 1 by unpacking.
        request_obj = stub_used._method
        request_pb, = request_obj.request_pbs
        self.assertTrue(isinstance(request_pb, request_type))
        self.assertEqual(request_obj.request_timeouts, [10])
        return request_pb

    def test_list_zones(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)

        PROJECT_ID = 'PROJECT_ID'

        def rpc_method(connection):
            return connection.list_zones(PROJECT_ID)

        method_name = 'ListZones'
        credentials, stubs = self._rpc_method_test_helper(rpc_method,
                                                          method_name)
        request_type = messages.ListZonesRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name, 'projects/PROJECT_ID')

    def test_get_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

        def rpc_method(connection):
            return connection.get_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        method_name = 'GetCluster'
        credentials, stubs = self._rpc_method_test_helper(rpc_method,
                                                          method_name)
        request_type = messages.GetClusterRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name,
                         'projects/PROJECT_ID/zones/ZONE/clusters/CLUSTER_ID')

    def test_list_clusters(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)

        PROJECT_ID = 'PROJECT_ID'

        def rpc_method(connection):
            return connection.list_clusters(PROJECT_ID)

        method_name = 'ListClusters'
        credentials, stubs = self._rpc_method_test_helper(rpc_method,
                                                          method_name)
        request_type = messages.ListClustersRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name, 'projects/PROJECT_ID')

    def test_create_cluster_with_defaults(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

        def rpc_method(connection):
            return connection.create_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        method_name = 'CreateCluster'
        credentials, stubs = self._rpc_method_test_helper(rpc_method,
                                                          method_name)
        request_type = messages.CreateClusterRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name, 'projects/PROJECT_ID/zones/ZONE')
        self.assertEqual(request_pb.cluster_id, CLUSTER_ID)
        cluster = request_pb.cluster
        self.assertEqual(cluster.serve_nodes, 3)
        self.assertEqual(
            set(field.name for field in cluster._fields.keys()),
            set(['serve_nodes']))

    def _create_cluster_helper_non_default_arguments(self, hdd_bytes=None,
                                                     ssd_bytes=None):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'
        DISPLAY_NAME = 'DISPLAY_NAME'
        SERVE_NODES = 8

        def rpc_method(connection):
            return connection.create_cluster(PROJECT_ID, ZONE, CLUSTER_ID,
                                             display_name=DISPLAY_NAME,
                                             serve_nodes=SERVE_NODES,
                                             ssd_bytes=ssd_bytes,
                                             hdd_bytes=hdd_bytes)

        method_name = 'CreateCluster'
        credentials, stubs = self._rpc_method_test_helper(rpc_method,
                                                          method_name)
        request_type = messages.CreateClusterRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name, 'projects/PROJECT_ID/zones/ZONE')
        self.assertEqual(request_pb.cluster_id, CLUSTER_ID)
        cluster = request_pb.cluster
        self.assertEqual(cluster.display_name, DISPLAY_NAME)
        self.assertEqual(cluster.serve_nodes, SERVE_NODES)

        all_fields = set(field.name for field in cluster._fields.keys())
        if ssd_bytes is not None:
            self.assertEqual(cluster.ssd_bytes, ssd_bytes)
            self.assertFalse('hdd_bytes' in all_fields)
            self.assertEqual(cluster.default_storage_type,
                             data_pb2.STORAGE_SSD)
        if hdd_bytes is not None:
            self.assertEqual(cluster.hdd_bytes, hdd_bytes)
            self.assertFalse('ssd_bytes' in all_fields)
            self.assertEqual(cluster.default_storage_type,
                             data_pb2.STORAGE_HDD)

    def test_create_cluster_non_default_args_with_ssd_bytes(self):
        self._create_cluster_helper_non_default_arguments(ssd_bytes=1024)

    def test_create_cluster_non_default_args_with_hdd_bytes(self):
        self._create_cluster_helper_non_default_arguments(hdd_bytes=1024)

    def test_create_cluster_non_default_args_conflict_ssd_and_hdd(self):
        with self.assertRaises(ValueError):
            self._create_cluster_helper_non_default_arguments(ssd_bytes=1024,
                                                              hdd_bytes=1024)

    def test_update_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.update_cluster,
                          project_name, zone_name, cluster_name)

    def test_delete_cluster(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)

        PROJECT_ID = 'PROJECT_ID'
        ZONE = 'ZONE'
        CLUSTER_ID = 'CLUSTER_ID'

        def rpc_method(connection):
            return connection.delete_cluster(PROJECT_ID, ZONE, CLUSTER_ID)

        method_name = 'DeleteCluster'
        credentials, stubs = self._rpc_method_test_helper(rpc_method,
                                                          method_name)
        request_type = messages.DeleteClusterRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name,
                         'projects/PROJECT_ID/zones/ZONE/clusters/CLUSTER_ID')

    def test_undelete_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.undelete_cluster,
                          project_name, zone_name, cluster_name)


class _MockMethod(object):

    def __init__(self, stub, result):
        self.stub = stub
        self.result = result
        self.request_pbs = []
        self.request_timeouts = []

    def async(self, request_pb, timeout_seconds):
        from gcloud_bigtable._testing import _StubMockResponse
        self.request_pbs.append(request_pb)
        self.request_timeouts.append(timeout_seconds)
        return _StubMockResponse(self, self.result)


class _ClusterStubMock(_StubMock):

    def __init__(self, credentials, result, method_name):
        super(_ClusterStubMock, self).__init__(credentials)
        self._method = _MockMethod(self, result)
        setattr(self, method_name, self._method)
