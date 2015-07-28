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


class Test__project_id_from_environment(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.client import _project_id_from_environment
        return _project_id_from_environment()

    def test_it(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        fake_project_id = object()
        mock_os = _MockWithAttachedMethods(fake_project_id)
        with _Monkey(MUT, os=mock_os):
            result = self._callFUT()

        self.assertTrue(result is fake_project_id)
        self.assertEqual(mock_os._called,
                         [('getenv', (MUT.PROJECT_ENV_VAR,), {})])


class Test__project_id_from_app_engine(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.client import _project_id_from_app_engine
        return _project_id_from_app_engine()

    def test_without_app_engine(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        with _Monkey(MUT, app_identity=None):
            result = self._callFUT()

        self.assertEqual(result, None)

    def test_with_app_engine(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        fake_project_id = object()
        mock_app_identity = _MockWithAttachedMethods(fake_project_id)
        with _Monkey(MUT, app_identity=mock_app_identity):
            result = self._callFUT()

        self.assertTrue(result is fake_project_id)
        self.assertEqual(mock_app_identity._called,
                         [('get_application_id', (), {})])


class Test__project_id_from_compute_engine(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.client import _project_id_from_compute_engine
        return _project_id_from_compute_engine()

    @staticmethod
    def _make_http_connection_response(status_code, read_result,
                                       raise_socket_err=False):
        import socket

        class Response(object):
            status = status_code

            @staticmethod
            def read():
                if raise_socket_err:
                    raise socket.error('Failed')
                else:
                    return read_result
        return Response

    @staticmethod
    def _make_fake_six_module(mock_http_client):
        class MockSix(object):
            class moves(object):
                http_client = mock_http_client
        return MockSix

    def _helper(self, status, raise_socket_err=False):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        fake_project_id = object()
        response = self._make_http_connection_response(
            status, fake_project_id, raise_socket_err=raise_socket_err)
        # The connection does the bulk of the work.
        mock_connection = _MockWithAttachedMethods(None, response, None)
        # The http_client module holds the connection constructor.
        mock_http_client = _MockWithAttachedMethods(mock_connection)
        # We need to put the client in place of it's location in six.
        mock_six = self._make_fake_six_module(mock_http_client)

        with _Monkey(MUT, six=mock_six):
            result = self._callFUT()

        if status == 200 and not raise_socket_err:
            self.assertEqual(result, fake_project_id)
        else:
            self.assertEqual(result, None)

        self.assertEqual(mock_connection._called, [
            (
                'request',
                ('GET', '/computeMetadata/v1/project/project-id'),
                {'headers': {'Metadata-Flavor': 'Google'}},
            ),
            (
                'getresponse',
                (),
                {},
            ),
            (
                'close',
                (),
                {},
            ),
        ])
        self.assertEqual(mock_http_client._called, [
            (
                'HTTPConnection',
                ('169.254.169.254',),
                {'timeout': 0.1},
            ),
        ])

    def test_success(self):
        self._helper(200)

    def test_failed_status(self):
        self._helper(404)

    def test_read_fails_with_socket_error(self):
        self._helper(200, raise_socket_err=True)


class Test__determine_project_id(unittest2.TestCase):

    def _callFUT(self, project_id):
        from gcloud_bigtable.client import _determine_project_id
        return _determine_project_id(project_id)

    def _helper(self, num_mocks_called, mock_output, method_input):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        mock_project_id_from_environment = _MockCalled(None)
        mock_project_id_from_app_engine = _MockCalled(None)
        mock_project_id_from_compute_engine = _MockCalled(None)

        monkey_kwargs = {
            '_project_id_from_environment': mock_project_id_from_environment,
            '_project_id_from_app_engine': mock_project_id_from_app_engine,
            '_project_id_from_compute_engine': (
                mock_project_id_from_compute_engine),
        }
        # Need the mocks in order they are called, so we can
        # access them based on `num_mocks_called`.
        mocks = [
            mock_project_id_from_environment,
            mock_project_id_from_app_engine,
            mock_project_id_from_compute_engine,
        ]
        mocks[num_mocks_called - 1].result = mock_output

        with _Monkey(MUT, **monkey_kwargs):
            if num_mocks_called == 3 and mock_output is None:
                with self.assertRaises(EnvironmentError):
                    self._callFUT(method_input)
            else:
                result = self._callFUT(method_input)
                self.assertEqual(result, method_input or mock_output)

        # Make sure our mocks were called with no arguments.
        for mock in mocks[:num_mocks_called]:
            mock.check_called(self, [()])
        for mock in mocks[num_mocks_called:]:
            mock.check_called(self, [])

    def test_fail_to_infer(self):
        self._helper(num_mocks_called=3, mock_output=None,
                     method_input=None)

    def test_with_explicit_value(self):
        self._helper(num_mocks_called=0, mock_output=None,
                     method_input=PROJECT_ID)

    def test_from_environment(self):
        self._helper(num_mocks_called=1, mock_output=PROJECT_ID,
                     method_input=None)

    def test_from_app_engine(self):
        self._helper(num_mocks_called=2, mock_output=PROJECT_ID,
                     method_input=None)

    def test_from_compute_engine(self):
        self._helper(num_mocks_called=3, mock_output=PROJECT_ID,
                     method_input=None)


class TestClient(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import client as MUT
        cls._MUT = MUT
        cls._STUB_SCOPES = [MUT.DATA_SCOPE]
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
        from gcloud_bigtable.client import Client
        return Client

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _constructor_test_helper(self, expected_scopes, project_id=None,
                                 read_only=False, admin=False,
                                 user_agent=None):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        determined_project_id = object()
        mock_determine_project_id = _MockCalled(determined_project_id)
        with _Monkey(MUT, _determine_project_id=mock_determine_project_id):
            client = self._makeOne(credentials, project_id=project_id,
                                   read_only=read_only, admin=admin,
                                   user_agent=user_agent)

        self.assertTrue(client._credentials is scoped_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped', (expected_scopes,), {}),
        ])
        self.assertTrue(client._project_id is determined_project_id)
        self.assertEqual(client.timeout_seconds, MUT.DEFAULT_TIMEOUT_SECONDS)
        self.assertEqual(client.user_agent, user_agent)
        mock_determine_project_id.check_called(self, [(project_id,)])

    def test_constructor_default(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        mock_creds_class = _MockWithAttachedMethods(credentials)

        with _Monkey(MUT, GoogleCredentials=mock_creds_class):
            client = self._makeOne(project_id=PROJECT_ID)

        self.assertEqual(client.project_id, PROJECT_ID)
        self.assertTrue(client._credentials is scoped_creds)
        self.assertEqual(client.user_agent, MUT.DEFAULT_USER_AGENT)
        self.assertEqual(mock_creds_class._called,
                         [('get_application_default', (), {})])
        expected_scopes = [MUT.DATA_SCOPE]
        self.assertEqual(credentials._called, [
            ('create_scoped', (expected_scopes,), {}),
        ])

    def test_constructor_explicit_credentials(self):
        from gcloud_bigtable import client as MUT
        expected_scopes = [MUT.DATA_SCOPE]
        self._constructor_test_helper(expected_scopes)

    def test_constructor_with_explicit_project_id(self):
        from gcloud_bigtable import client as MUT
        expected_scopes = [MUT.DATA_SCOPE]
        self._constructor_test_helper(expected_scopes, project_id=PROJECT_ID)

    def test_constructor_with_explicit_user_agent(self):
        from gcloud_bigtable import client as MUT
        user_agent = 'USER_AGENT'
        expected_scopes = [MUT.DATA_SCOPE]
        self._constructor_test_helper(expected_scopes, user_agent=user_agent)

    def test_constructor_with_admin(self):
        from gcloud_bigtable import client as MUT
        expected_scopes = [MUT.DATA_SCOPE, MUT.ADMIN_SCOPE]
        self._constructor_test_helper(expected_scopes, admin=True)

    def test_constructor_with_read_only(self):
        from gcloud_bigtable import client as MUT
        expected_scopes = [MUT.READ_ONLY_SCOPE]
        self._constructor_test_helper(expected_scopes, read_only=True)

    def test_constructor_both_admin_and_read_only(self):
        with self.assertRaises(ValueError):
            self._makeOne(None, admin=True, read_only=True)

    def test_from_service_account_json(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        klass = self._getTargetClass()
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        get_adc = _MockCalled(credentials)
        json_credentials_path = 'JSON_CREDENTIALS_PATH'

        with _Monkey(MUT,
                     _get_application_default_credential_from_file=get_adc):
            client = klass.from_service_account_json(
                json_credentials_path, project_id=PROJECT_ID)

        self.assertEqual(client.project_id, PROJECT_ID)
        self.assertTrue(client._credentials is scoped_creds)

        expected_scopes = [MUT.DATA_SCOPE]
        self.assertEqual(credentials._called, [
            ('create_scoped', (expected_scopes,), {}),
        ])
        # _get_application_default_credential_from_file only has pos. args.
        get_adc.check_called(self, [(json_credentials_path,)])

    def test_from_service_account_p12(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        klass = self._getTargetClass()
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        signed_creds = _MockCalled(credentials)

        private_key = 'PRIVATE_KEY'
        mock_get_contents = _MockCalled(private_key)
        client_email = 'CLIENT_EMAIL'
        private_key_path = 'PRIVATE_KEY_PATH'

        with _Monkey(MUT, SignedJwtAssertionCredentials=signed_creds,
                     _get_contents=mock_get_contents):
            client = klass.from_service_account_p12(
                client_email, private_key_path, project_id=PROJECT_ID)

        self.assertEqual(client.project_id, PROJECT_ID)
        self.assertTrue(client._credentials is scoped_creds)
        expected_scopes = [MUT.DATA_SCOPE]
        self.assertEqual(credentials._called, [
            ('create_scoped', (expected_scopes,), {}),
        ])
        # SignedJwtAssertionCredentials() called with only kwargs
        signed_creds_kw = {
            'private_key': private_key,
            'service_account_name': client_email,
        }
        signed_creds.check_called(self, [()], [signed_creds_kw])
        # Load private key (via _get_contents) from the key path.
        mock_get_contents.check_called(self, [(private_key_path,)])

    def test_credentials_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(credentials, project_id=PROJECT_ID)
        self.assertTrue(client.credentials is scoped_creds)

    def test_project_id_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(credentials, project_id=PROJECT_ID)
        self.assertEqual(client.project_id, PROJECT_ID)

    def test_project_name_property(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        project_name = 'projects/' + PROJECT_ID
        client = self._makeOne(credentials, project_id=PROJECT_ID)
        self.assertEqual(client.project_name, project_name)

    def test_cluster_factory(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.cluster import Cluster

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(credentials, project_id=PROJECT_ID)

        zone = 'zone'
        cluster_id = 'cluster-id'
        cluster = client.cluster(zone, cluster_id)
        self.assertTrue(isinstance(cluster, Cluster))
        self.assertTrue(cluster.client is client)
        self.assertEqual(cluster.zone, zone)
        self.assertEqual(cluster.cluster_id, cluster_id)

    def _list_zones_helper(self, zone_status):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)

        request_pb = messages_pb2.ListZonesRequest(
            name='projects/' + PROJECT_ID,
        )
        zone1 = 'foo'
        zone2 = 'foo'
        response_pb = messages_pb2.ListZonesResponse(
            zones=[
                data_pb2.Zone(display_name=zone1, status=zone_status),
                data_pb2.Zone(display_name=zone2, status=zone_status),
            ],
        )
        expected_result = [zone1, zone2]

        # Create the method to be performed on the client.
        timeout_seconds = 281330

        def result_method(client):
            return client.list_zones(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('ListZones', result_method, request_pb,
                                      response_pb, expected_result, PROJECT_ID,
                                      timeout_seconds=timeout_seconds)

    def test_list_zones(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        self._list_zones_helper(data_pb2.Zone.OK)

    def test_list_zones_failure(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        with self.assertRaises(ValueError):
            self._list_zones_helper(data_pb2.Zone.EMERGENCY_MAINENANCE)

    def test_list_clusters(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable.cluster import Cluster

        # Create request_pb
        request_pb = messages_pb2.ListClustersRequest(
            name='projects/' + PROJECT_ID,
        )

        # Create response_pb
        zone = 'foo'
        failed_zone = 'bar'
        cluster_id1 = 'cluster-id1'
        cluster_id2 = 'cluster-id2'
        cluster_name1 = ('projects/' + PROJECT_ID + '/zones/' + zone +
                         '/clusters/' + cluster_id1)
        cluster_name2 = ('projects/' + PROJECT_ID + '/zones/' + zone +
                         '/clusters/' + cluster_id2)
        response_pb = messages_pb2.ListClustersResponse(
            failed_zones=[
                data_pb2.Zone(display_name=failed_zone),
            ],
            clusters=[
                data_pb2.Cluster(
                    name=cluster_name1,
                    display_name=cluster_name1,
                    serve_nodes=3,
                ),
                data_pb2.Cluster(
                    name=cluster_name2,
                    display_name=cluster_name2,
                    serve_nodes=3,
                ),
            ],
        )

        # Create expected_result.
        failed_zones = [failed_zone]
        clusters = [
            Cluster(zone, cluster_id1, None),
            Cluster(zone, cluster_id2, None),
        ]
        expected_result = (clusters, failed_zones)

        # We didn't have access to the client above when creating the clusters
        # so we will patch it in the `result_method` closure.
        timeout_seconds = 8004

        def result_method(client):
            clusters[0]._client = client
            clusters[1]._client = client
            return client.list_clusters(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('ListClusters', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)


class Test__get_contents(unittest2.TestCase):

    def _callFUT(self, filename):
        from gcloud_bigtable.client import _get_contents
        return _get_contents(filename)

    def test_it(self):
        import tempfile

        filename = tempfile.mktemp()
        contents = b'foobar'
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)

        self.assertEqual(self._callFUT(filename), contents)
