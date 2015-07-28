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

    def test_constructor(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        credentials = _MockWithAttachedMethods()
        transformer = self._makeOne(credentials)
        self.assertTrue(transformer._credentials is credentials)
        self.assertEqual(credentials._called, [])

    def test___call__(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        access_token_expected = 'FOOBARBAZ'

        class _ReturnVal(object):
            access_token = access_token_expected

        credentials = _MockWithAttachedMethods(_ReturnVal)
        transformer = self._makeOne(credentials)
        result = transformer(None)
        self.assertEqual(
            result,
            [('Authorization', 'Bearer ' + access_token_expected)])
        self.assertEqual(credentials._called, [('get_access_token', (), {})])


class TestConnection(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.connection import Connection
        return Connection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(True, scoped_creds)
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is scoped_creds)
        klass = self._getTargetClass()
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
            ('create_scoped', ((klass.SCOPE,),), {}),
        ])

    def test__create_scoped_credentials_with_no_credentials(self):
        klass = self._getTargetClass()
        credentials = None
        scope = object()

        new_credentials = klass._create_scoped_credentials(credentials, scope)
        self.assertTrue(new_credentials is credentials)

    def test__create_scoped_credentials_with_credentials(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        klass = self._getTargetClass()
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(True, scoped_creds)
        scope = object()

        new_credentials = klass._create_scoped_credentials(credentials, scope)
        self.assertTrue(new_credentials is scoped_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
            ('create_scoped', (scope,), {}),
        ])

    def test_credentials_property(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        credentials = _MockWithAttachedMethods(False)
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection.credentials is credentials)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])


class Test_make_stub(unittest2.TestCase):

    def _callFUT(self, credentials, stub_factory, host, port):
        from gcloud_bigtable.connection import make_stub
        return make_stub(credentials, stub_factory, host, port)

    def test_it(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import connection as MUT

        mock_result = object()
        custom_factory = _MockCalled(mock_result)
        transformed = object()
        transformer = _MockCalled(transformed)

        host = 'HOST'
        port = 1025
        certs = 'FOOBAR'
        credentials = _MockWithAttachedMethods()
        with _Monkey(MUT, get_certs=lambda: certs,
                     MetadataTransformer=transformer):
            result = self._callFUT(credentials, custom_factory, host, port)

        self.assertTrue(result is mock_result)
        custom_factory.check_called(
            self,
            [(host, port)],
            [{
                'metadata_transformer': transformed,
                'secure': True,
                'root_certificates': certs,
            }],
        )
        transformer.check_called(self, [(credentials,)])
        self.assertEqual(credentials._called, [])
