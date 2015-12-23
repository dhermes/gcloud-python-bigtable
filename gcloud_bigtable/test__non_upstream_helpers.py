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
        from gcloud_bigtable._non_upstream_helpers import (
            _project_from_environment)
        return _project_from_environment()

    def test_it(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _non_upstream_helpers as MUT

        fake_project = object()
        mock_os = _MockWithAttachedMethods(fake_project)
        with _Monkey(MUT, os=mock_os):
            result = self._callFUT()

        self.assertTrue(result is fake_project)
        self.assertEqual(mock_os._called,
                         [('getenv', (MUT.PROJECT_ENV_VAR,), {})])


class Test__project_from_app_engine(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable._non_upstream_helpers import (
            _project_from_app_engine)
        return _project_from_app_engine()

    def test_without_app_engine(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _non_upstream_helpers as MUT

        with _Monkey(MUT, app_identity=None):
            result = self._callFUT()

        self.assertEqual(result, None)

    def test_with_app_engine(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _non_upstream_helpers as MUT

        fake_project = object()
        mock_app_identity = _MockWithAttachedMethods(fake_project)
        with _Monkey(MUT, app_identity=mock_app_identity):
            result = self._callFUT()

        self.assertTrue(result is fake_project)
        self.assertEqual(mock_app_identity._called,
                         [('get_application_id', (), {})])


class Test__project_from_compute_engine(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable._non_upstream_helpers import (
            _project_from_compute_engine)
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
        from gcloud_bigtable import _non_upstream_helpers as MUT

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
        from gcloud_bigtable._non_upstream_helpers import _determine_project
        return _determine_project(project)

    def _helper(self, num_mocks_called, mock_output, method_input):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _non_upstream_helpers as MUT

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


class Test__get_contents(unittest2.TestCase):

    def _callFUT(self, filename):
        from gcloud_bigtable._non_upstream_helpers import _get_contents
        return _get_contents(filename)

    def test_it(self):
        import tempfile

        filename = tempfile.mktemp()
        contents = b'foobar'
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)

        self.assertEqual(self._callFUT(filename), contents)


class Test__FactoryMixin(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable._non_upstream_helpers import _FactoryMixin
        return _FactoryMixin

    def test_from_service_account_json(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _non_upstream_helpers as MUT

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
        self.assertTrue(client._credentials is credentials)

        self.assertEqual(credentials._called, [])
        # _get_application_default_credential_from_file only has pos. args.
        get_adc.check_called(self, [(json_credentials_path,)])

    def test_from_service_account_p12(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _non_upstream_helpers as MUT

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
        self.assertTrue(client._credentials is credentials)
        self.assertEqual(credentials._called, [])
        # SignedJwtAssertionCredentials() called with only kwargs
        signed_creds_kw = {
            'private_key': private_key,
            'service_account_name': client_email,
            'scope': None,
        }
        signed_creds.check_called(self, [()], [signed_creds_kw])
        # Load private key (via _get_contents) from the key path.
        mock_get_contents.check_called(self, [(private_key_path,)])


class Test__timestamp_to_microseconds(unittest2.TestCase):

    def _callFUT(self, timestamp, granularity=1000):
        from gcloud_bigtable._non_upstream_helpers import (
            _timestamp_to_microseconds)
        return _timestamp_to_microseconds(timestamp, granularity=granularity)

    def test_default_granularity(self):
        import datetime
        from gcloud_bigtable import _non_upstream_helpers as MUT

        microseconds = 898294371
        millis_granularity = microseconds - (microseconds % 1000)
        timestamp = MUT.EPOCH + datetime.timedelta(microseconds=microseconds)
        self.assertEqual(millis_granularity, self._callFUT(timestamp))

    def test_no_granularity(self):
        import datetime
        from gcloud_bigtable import _non_upstream_helpers as MUT

        microseconds = 11122205067
        timestamp = MUT.EPOCH + datetime.timedelta(microseconds=microseconds)
        self.assertEqual(microseconds, self._callFUT(timestamp, granularity=1))

    def test_non_utc_timestamp(self):
        import datetime

        epoch_no_tz = datetime.datetime.utcfromtimestamp(0)
        with self.assertRaises(TypeError):
            self._callFUT(epoch_no_tz)

    def test_non_datetime_timestamp(self):
        timestamp = object()  # Not a datetime object.
        with self.assertRaises(TypeError):
            self._callFUT(timestamp)


class Test__microseconds_to_timestamp(unittest2.TestCase):

    def _callFUT(self, microseconds):
        from gcloud_bigtable._non_upstream_helpers import (
            _microseconds_to_timestamp)
        return _microseconds_to_timestamp(microseconds)

    def test_it(self):
        import datetime
        from gcloud_bigtable import _non_upstream_helpers as MUT

        microseconds = 123456
        timestamp = MUT.EPOCH + datetime.timedelta(microseconds=microseconds)
        self.assertEqual(timestamp, self._callFUT(microseconds))


class Test__to_bytes(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable._non_upstream_helpers import _to_bytes
        return _to_bytes(*args, **kwargs)

    def test_with_bytes(self):
        value = b'bytes-val'
        self.assertEqual(self._callFUT(value), value)

    def test_with_unicode(self):
        value = u'string-val'
        encoded_value = b'string-val'
        self.assertEqual(self._callFUT(value), encoded_value)

    def test_unicode_non_ascii(self):
        value = u'\u2013'  # Long hyphen
        encoded_value = b'\xe2\x80\x93'
        self.assertRaises(UnicodeEncodeError, self._callFUT, value)
        self.assertEqual(self._callFUT(value, encoding='utf-8'),
                         encoded_value)

    def test_with_nonstring_type(self):
        value = object()
        self.assertRaises(TypeError, self._callFUT, value)
