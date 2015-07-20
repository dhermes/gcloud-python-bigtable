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

    def test_list_zones(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages)
        from gcloud_bigtable._testing import _Credentials
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster_connection as MUT
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        stubs = []
        expected_result = object()

        def mock_make_stub(creds):
            stub = _ClusterStubMock(creds, expected_result)
            stubs.append(stub)
            return stub

        with _Monkey(MUT, make_cluster_stub=mock_make_stub):
            project_id = 'PROJECT_ID'
            result = connection.list_zones(project_id)

        self.assertTrue(result is expected_result)

        # Asserting length 1 by unpacking.
        stub_used, = stubs
        self.assertTrue(stub_used._credentials is credentials)
        self.assertEqual(stub_used._enter_calls, 1)

        # Asserting length 1 (and a 3-tuple) by unpacking.
        (exc_type, exc_val, _), = stub_used._exit_args
        self.assertTrue(exc_type is None)
        self.assertTrue(isinstance(exc_val, type(None)))

        # Asserting length 1 by unpacking.
        request_pb, = stub_used.ListZones.request_pbs
        self.assertTrue(isinstance(request_pb, messages.ListZonesRequest))
        self.assertEqual(request_pb.name, 'projects/PROJECT_ID')
        self.assertEqual(stub_used.ListZones.request_timeouts, [10])

    def test_get_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.get_cluster,
                          project_name, zone_name, cluster_name)

    def test_list_clusters(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        self.assertRaises(NotImplementedError, connection.list_clusters,
                          project_name)

    def test_create_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        self.assertRaises(NotImplementedError, connection.create_cluster,
                          project_name, zone_name)

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
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.delete_cluster,
                          project_name, zone_name, cluster_name)

    def test_undelete_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.undelete_cluster,
                          project_name, zone_name, cluster_name)


class _ListZonesMethod(object):

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

    def __init__(self, credentials, result):
        super(_ClusterStubMock, self).__init__(credentials)
        self.ListZones = _ListZonesMethod(self, result)
