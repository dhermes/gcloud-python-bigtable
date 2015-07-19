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


class TestMetadataTransformer(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.connection import MetadataTransformer
        return MetadataTransformer

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_ctor(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        transformer = self._makeOne(credentials)
        self.assertTrue(transformer._credentials is credentials)

    def test___call__(self):
        from gcloud_bigtable._testing import _Credentials

        access_token_expected = 'FOOBARBAZ'

        class _ReturnVal(object):

            access_token = access_token_expected

        class _CustomCreds(_Credentials):

            @staticmethod
            def get_access_token():
                return _ReturnVal

        credentials = _CustomCreds()
        transformer = self._makeOne(credentials)
        result = transformer(None)
        self.assertEqual(
            result,
            [('Authorization', 'Bearer ' + access_token_expected)])


class TestConnection(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.connection import Connection
        return Connection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (None,))

    def test__create_scoped_credentials_with_no_credentials(self):
        klass = self._getTargetClass()
        credentials = None
        scope = object()

        new_credentials = klass._create_scoped_credentials(credentials, scope)
        self.assertTrue(new_credentials is credentials)

    def test__create_scoped_credentials_with_credentials(self):
        from gcloud_bigtable._testing import _Credentials
        klass = self._getTargetClass()
        credentials = _Credentials()
        scope = object()

        new_credentials = klass._create_scoped_credentials(credentials, scope)
        self.assertTrue(new_credentials is credentials)
        self.assertTrue(credentials._scopes is scope)

    def test_credentials_property(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection.credentials is credentials)


class Test__set_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.connection import _set_certs
        return _set_certs()

    def test_it(self):
        import tempfile
        from gcloud_bigtable import connection as MUT

        self.assertTrue(MUT.AuthInfo.ROOT_CERTIFICATES is None)

        original_SSL_CERT_FILE = MUT.SSL_CERT_FILE
        filename = tempfile.mktemp()
        contents = b'FOOBARBAZ'
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)
        try:
            MUT.SSL_CERT_FILE = filename
            self._callFUT()
        finally:
            MUT.SSL_CERT_FILE = original_SSL_CERT_FILE

        self.assertEqual(MUT.AuthInfo.ROOT_CERTIFICATES, contents)


class Test_set_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.connection import set_certs
        return set_certs()

    def test_call_private(self):
        from gcloud_bigtable import connection as MUT

        orig_certs = MUT.AuthInfo.ROOT_CERTIFICATES
        orig__set_certs = MUT._set_certs

        call_count = [0]

        def mock_set_certs():
            call_count[0] += 1

        try:
            MUT.AuthInfo.ROOT_CERTIFICATES = None
            MUT._set_certs = mock_set_certs
            self._callFUT()
        finally:
            MUT.AuthInfo.ROOT_CERTIFICATES = orig_certs
            MUT._set_certs = orig__set_certs

        self.assertEqual(call_count, [1])

    def test_do_nothing(self):
        from gcloud_bigtable import connection as MUT

        orig_certs = MUT.AuthInfo.ROOT_CERTIFICATES
        orig__set_certs = MUT._set_certs

        call_count = [0]

        def mock_set_certs():
            call_count[0] += 1

        # Make sure the fake method gets called by **someone** to make
        # tox -e cover happy.
        mock_set_certs()
        self.assertEqual(call_count, [1])
        try:
            MUT.AuthInfo.ROOT_CERTIFICATES = object()
            MUT._set_certs = mock_set_certs
            self._callFUT()
        finally:
            MUT.AuthInfo.ROOT_CERTIFICATES = orig_certs
            MUT._set_certs = orig__set_certs

        self.assertEqual(call_count, [1])


class Test_get_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable.connection import get_certs
        return get_certs()

    def test_it(self):
        from gcloud_bigtable import connection as MUT

        orig_certs = MUT.AuthInfo.ROOT_CERTIFICATES
        orig_set_certs = MUT.set_certs

        call_kwargs = []
        return_val = object()

        def mock_set_certs(**kwargs):
            call_kwargs.append(kwargs)

        try:
            MUT.AuthInfo.ROOT_CERTIFICATES = return_val
            MUT.set_certs = mock_set_certs
            result = self._callFUT()
        finally:
            MUT.AuthInfo.ROOT_CERTIFICATES = orig_certs
            MUT.set_certs = orig_set_certs

        self.assertEqual(call_kwargs, [{'reset': False}])
        self.assertTrue(result is return_val)
