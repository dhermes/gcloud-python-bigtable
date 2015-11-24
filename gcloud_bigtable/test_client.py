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


PROJECT = 'project-id'


class Test__project_from_environment(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.client import _project_from_environment
        return _project_from_environment()

    def test_it(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        fake_project = object()
        mock_os = _MockWithAttachedMethods(fake_project)
        with _Monkey(MUT, os=mock_os):
            result = self._callFUT()

        self.assertTrue(result is fake_project)
        self.assertEqual(mock_os._called,
                         [('getenv', (MUT.PROJECT_ENV_VAR,), {})])


class Test__project_from_app_engine(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.client import _project_from_app_engine
        return _project_from_app_engine()

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

        fake_project = object()
        mock_app_identity = _MockWithAttachedMethods(fake_project)
        with _Monkey(MUT, app_identity=mock_app_identity):
            result = self._callFUT()

        self.assertTrue(result is fake_project)
        self.assertEqual(mock_app_identity._called,
                         [('get_application_id', (), {})])


class Test__project_from_compute_engine(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.client import _project_from_compute_engine
        return _project_from_compute_engine()

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

        fake_project = object()
        response = self._make_http_connection_response(
            status, fake_project, raise_socket_err=raise_socket_err)
        # The connection does the bulk of the work.
        mock_connection = _MockWithAttachedMethods(None, response, None)
        # The http_client module holds the connection constructor.
        mock_http_client = _MockWithAttachedMethods(mock_connection)
        # We need to put the client in place of it's location in six.
        mock_six = self._make_fake_six_module(mock_http_client)

        with _Monkey(MUT, six=mock_six):
            result = self._callFUT()

        if status == 200 and not raise_socket_err:
            self.assertEqual(result, fake_project)
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


class Test__determine_project(unittest2.TestCase):

    def _callFUT(self, project):
        from gcloud_bigtable.client import _determine_project
        return _determine_project(project)

    def _helper(self, num_mocks_called, mock_output, method_input):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        mock_project_from_environment = _MockCalled(None)
        mock_project_from_app_engine = _MockCalled(None)
        mock_project_from_compute_engine = _MockCalled(None)

        monkey_kwargs = {
            '_project_from_environment': mock_project_from_environment,
            '_project_from_app_engine': mock_project_from_app_engine,
            '_project_from_compute_engine': (
                mock_project_from_compute_engine),
        }
        # Need the mocks in order they are called, so we can
        # access them based on `num_mocks_called`.
        mocks = [
            mock_project_from_environment,
            mock_project_from_app_engine,
            mock_project_from_compute_engine,
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
                     method_input=PROJECT)

    def test_from_environment(self):
        self._helper(num_mocks_called=1, mock_output=PROJECT,
                     method_input=None)

    def test_from_app_engine(self):
        self._helper(num_mocks_called=2, mock_output=PROJECT,
                     method_input=None)

    def test_from_compute_engine(self):
        self._helper(num_mocks_called=3, mock_output=PROJECT,
                     method_input=None)


class TestClient(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.client import Client
        return Client

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _constructor_test_helper(self, expected_scopes, project=None,
                                 read_only=False, admin=False,
                                 user_agent=None):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        determined_project = object()
        mock_determine_project = _MockCalled(determined_project)
        with _Monkey(MUT, _determine_project=mock_determine_project):
            client = self._makeOne(project=project, credentials=credentials,
                                   read_only=read_only, admin=admin,
                                   user_agent=user_agent)

        self.assertTrue(client._credentials is scoped_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped', (expected_scopes,), {}),
        ])
        self.assertTrue(client.project is determined_project)
        self.assertEqual(client.timeout_seconds, MUT.DEFAULT_TIMEOUT_SECONDS)
        self.assertEqual(client.user_agent, user_agent)
        mock_determine_project.check_called(self, [(project,)])

    def test_constructor_default(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        mock_creds_class = _MockWithAttachedMethods(credentials)

        with _Monkey(MUT, GoogleCredentials=mock_creds_class):
            client = self._makeOne(project=PROJECT)

        self.assertEqual(client.project, PROJECT)
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

    def test_constructor_with_explicit_project(self):
        from gcloud_bigtable import client as MUT
        expected_scopes = [MUT.DATA_SCOPE]
        self._constructor_test_helper(expected_scopes, project=PROJECT)

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
            self._makeOne(project=PROJECT, credentials=None,
                          admin=True, read_only=True)

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
                json_credentials_path, project=PROJECT)

        self.assertEqual(client.project, PROJECT)
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
                client_email, private_key_path, project=PROJECT)

        self.assertEqual(client.project, PROJECT)
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

    def _copy_test_helper(self, read_only=False, admin=False):
        class Credentials(object):

            scopes = None

            def __init__(self, value=None):
                self.value = value

            def create_scoped(self, scope):
                self.scopes = scope
                return self

            def __eq__(self, other):
                return self.value == other.value

        credentials = Credentials('value')
        timeout_seconds = 123
        user_agent = 'you-sir-age-int'
        client = self._makeOne(project=PROJECT, credentials=credentials,
                               read_only=read_only, admin=admin,
                               timeout_seconds=timeout_seconds,
                               user_agent=user_agent)
        # Put some fake stubs in place so that we can verify they
        # don't get copied.
        client._data_stub_internal = object()
        client._cluster_stub_internal = object()
        client._operations_stub_internal = object()
        client._table_stub_internal = object()

        new_client = client.copy()
        self.assertEqual(new_client._admin, client._admin)
        self.assertEqual(new_client._credentials, client._credentials)
        # Make sure credentials (a non-simple type) gets copied
        # to a new instance.
        self.assertFalse(new_client._credentials is client._credentials)
        self.assertEqual(new_client.project, client.project)
        self.assertEqual(new_client.user_agent, client.user_agent)
        self.assertEqual(new_client.timeout_seconds, client.timeout_seconds)
        # Make sure stubs are not preserved.
        self.assertEqual(new_client._data_stub_internal, None)
        self.assertEqual(new_client._cluster_stub_internal, None)
        self.assertEqual(new_client._operations_stub_internal, None)
        self.assertEqual(new_client._table_stub_internal, None)

    def test_copy(self):
        self._copy_test_helper()

    def test_copy_admin(self):
        self._copy_test_helper(admin=True)

    def test_copy_read_only(self):
        self._copy_test_helper(read_only=True)

    def test_credentials_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        self.assertTrue(client.credentials is scoped_creds)

    def test_project_name_property(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        project_name = 'projects/' + PROJECT
        client = self._makeOne(project=PROJECT, credentials=credentials)
        self.assertEqual(client.project_name, project_name)

    def test_data_stub_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        client._data_stub_internal = object()
        self.assertTrue(client._data_stub is client._data_stub_internal)

    def test_data_stub_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        with self.assertRaises(ValueError):
            getattr(client, '_data_stub')

    def test_cluster_stub_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)
        client._cluster_stub_internal = object()
        self.assertTrue(client._cluster_stub is client._cluster_stub_internal)

    def test_cluster_stub_non_admin_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=False)
        with self.assertRaises(ValueError):
            getattr(client, '_cluster_stub')

    def test_cluster_stub_unset_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)
        with self.assertRaises(ValueError):
            getattr(client, '_cluster_stub')

    def test_operations_stub_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)
        client._operations_stub_internal = object()
        self.assertTrue(client._operations_stub is
                        client._operations_stub_internal)

    def test_operations_stub_non_admin_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=False)
        with self.assertRaises(ValueError):
            getattr(client, '_operations_stub')

    def test_operations_stub_unset_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)
        with self.assertRaises(ValueError):
            getattr(client, '_operations_stub')

    def test_table_stub_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)
        client._table_stub_internal = object()
        self.assertTrue(client._table_stub is client._table_stub_internal)

    def test_table_stub_non_admin_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=False)
        with self.assertRaises(ValueError):
            getattr(client, '_table_stub')

    def test_table_stub_unset_failure(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)
        with self.assertRaises(ValueError):
            getattr(client, '_table_stub')

    def test__make_data_stub(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT
        from gcloud_bigtable.client import DATA_API_HOST
        from gcloud_bigtable.client import DATA_API_PORT
        from gcloud_bigtable.client import DATA_STUB_FACTORY

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        expected_result = object()
        mock_make_stub = _MockCalled(expected_result)
        with _Monkey(MUT, make_stub=mock_make_stub):
            result = client._make_data_stub()

        self.assertTrue(result is expected_result)
        make_stub_args = [
            (
                client,
                DATA_STUB_FACTORY,
                DATA_API_HOST,
                DATA_API_PORT,
            ),
        ]
        mock_make_stub.check_called(self, make_stub_args)

    def test__make_cluster_stub(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT
        from gcloud_bigtable.client import CLUSTER_ADMIN_HOST
        from gcloud_bigtable.client import CLUSTER_ADMIN_PORT
        from gcloud_bigtable.client import CLUSTER_STUB_FACTORY

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        expected_result = object()
        mock_make_stub = _MockCalled(expected_result)
        with _Monkey(MUT, make_beta_stub=mock_make_stub):
            result = client._make_cluster_stub()

        self.assertTrue(result is expected_result)
        make_stub_args = [
            (
                client,
                CLUSTER_STUB_FACTORY,
                CLUSTER_ADMIN_HOST,
                CLUSTER_ADMIN_PORT,
            ),
        ]
        mock_make_stub.check_called(self, make_stub_args)

    def test__make_operations_stub(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT
        from gcloud_bigtable.client import CLUSTER_ADMIN_HOST
        from gcloud_bigtable.client import CLUSTER_ADMIN_PORT
        from gcloud_bigtable.client import OPERATIONS_STUB_FACTORY

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        expected_result = object()
        mock_make_stub = _MockCalled(expected_result)
        with _Monkey(MUT, make_beta_stub=mock_make_stub):
            result = client._make_operations_stub()

        self.assertTrue(result is expected_result)
        make_stub_args = [
            (
                client,
                OPERATIONS_STUB_FACTORY,
                CLUSTER_ADMIN_HOST,
                CLUSTER_ADMIN_PORT,
            ),
        ]
        mock_make_stub.check_called(self, make_stub_args)

    def test__make_table_stub(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT
        from gcloud_bigtable.client import TABLE_ADMIN_HOST
        from gcloud_bigtable.client import TABLE_ADMIN_PORT
        from gcloud_bigtable.client import TABLE_STUB_FACTORY

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        expected_result = object()
        mock_make_stub = _MockCalled(expected_result)
        with _Monkey(MUT, make_stub=mock_make_stub):
            result = client._make_table_stub()

        self.assertTrue(result is expected_result)
        make_stub_args = [
            (
                client,
                TABLE_STUB_FACTORY,
                TABLE_ADMIN_HOST,
                TABLE_ADMIN_PORT,
            ),
        ]
        mock_make_stub.check_called(self, make_stub_args)

    def test_is_started(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        self.assertFalse(client.is_started())
        client._data_stub_internal = object()
        self.assertTrue(client.is_started())
        client._data_stub_internal = None
        self.assertFalse(client.is_started())

    def _start_method_helper(self, admin):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import client as MUT

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=admin)
        stub = _FakeStub()
        mock_make_stub = _MockCalled(stub)
        with _Monkey(MUT, make_stub=mock_make_stub,
                     make_beta_stub=mock_make_stub):
            client.start()

        self.assertTrue(client._data_stub_internal is stub)
        if admin:
            self.assertTrue(client._cluster_stub_internal is stub)
            self.assertTrue(client._operations_stub_internal is stub)
            self.assertTrue(client._table_stub_internal is stub)
            self.assertEqual(stub._entered, 4)
        else:
            self.assertTrue(client._cluster_stub_internal is None)
            self.assertTrue(client._operations_stub_internal is None)
            self.assertTrue(client._table_stub_internal is None)
            self.assertEqual(stub._entered, 1)
        self.assertEqual(stub._exited, [])

    def test_start_non_admin(self):
        self._start_method_helper(admin=False)

    def test_start_with_admin(self):
        self._start_method_helper(admin=True)

    def test_start_while_started(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        client._data_stub_internal = data_stub = object()
        self.assertTrue(client.is_started())
        client.start()

        # Make sure the stub did not change.
        self.assertEqual(client._data_stub_internal, data_stub)

    def _stop_method_helper(self, admin):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=admin)
        stub1 = _FakeStub()
        stub2 = _FakeStub()
        client._data_stub_internal = stub1
        client._cluster_stub_internal = stub2
        client._operations_stub_internal = stub2
        client._table_stub_internal = stub2
        client.stop()
        self.assertTrue(client._data_stub_internal is None)
        self.assertTrue(client._cluster_stub_internal is None)
        self.assertTrue(client._operations_stub_internal is None)
        self.assertTrue(client._table_stub_internal is None)
        self.assertEqual(stub1._entered, 0)
        self.assertEqual(stub2._entered, 0)
        exc_none_triple = (None, None, None)
        self.assertEqual(stub1._exited, [exc_none_triple])
        if admin:
            self.assertEqual(stub2._exited, [exc_none_triple] * 3)
        else:
            self.assertEqual(stub2._exited, [])

    def test_stop_non_admin(self):
        self._stop_method_helper(admin=False)

    def test_stop_with_admin(self):
        self._stop_method_helper(admin=True)

    def test_stop_while_stopped(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)
        self.assertFalse(client.is_started())

        # This is a bit hacky. We set the cluster stub protected value
        # since it isn't used in is_started() and make sure that stop
        # doesn't reset this value to None.
        client._cluster_stub_internal = cluster_stub = object()
        client.stop()
        # Make sure the cluster stub did not change.
        self.assertEqual(client._cluster_stub_internal, cluster_stub)

    def test_cluster_factory(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.cluster import Cluster

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT, credentials=credentials)

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
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)

        # Create request_pb
        request_pb = messages_pb2.ListZonesRequest(
            name='projects/' + PROJECT,
        )

        # Create response_pb
        zone1 = 'foo'
        zone2 = 'bar'
        response_pb = messages_pb2.ListZonesResponse(
            zones=[
                data_pb2.Zone(display_name=zone1, status=zone_status),
                data_pb2.Zone(display_name=zone2, status=zone_status),
            ],
        )

        # Patch the stub used by the API method.
        client._cluster_stub_internal = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = [zone1, zone2]

        # Perform the method and check the result.
        timeout_seconds = 281330
        result = client.list_zones(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'ListZones',
            (request_pb, timeout_seconds),
            {},
        )])

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
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(project=PROJECT,
                               credentials=credentials, admin=True)

        # Create request_pb
        request_pb = messages_pb2.ListClustersRequest(
            name='projects/' + PROJECT,
        )

        # Create response_pb
        zone = 'foo'
        failed_zone = 'bar'
        cluster_id1 = 'cluster-id1'
        cluster_id2 = 'cluster-id2'
        cluster_name1 = ('projects/' + PROJECT + '/zones/' + zone +
                         '/clusters/' + cluster_id1)
        cluster_name2 = ('projects/' + PROJECT + '/zones/' + zone +
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

        # Patch the stub used by the API method.
        client._cluster_stub_internal = stub = StubMock(response_pb)

        # Create expected_result.
        failed_zones = [failed_zone]
        clusters = [
            client.cluster(zone, cluster_id1),
            client.cluster(zone, cluster_id2),
        ]
        expected_result = (clusters, failed_zones)

        # Perform the method and check the result.
        timeout_seconds = 8004
        result = client.list_clusters(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'ListClusters',
            (request_pb, timeout_seconds),
            {},
        )])


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


class _FakeStub(object):

    def __init__(self):
        self._entered = 0
        self._exited = []

    def __enter__(self):
        self._entered += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exited.append((exc_type, exc_val, exc_tb))
        return True
