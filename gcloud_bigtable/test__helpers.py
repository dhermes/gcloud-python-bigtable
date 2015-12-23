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
        from gcloud_bigtable._helpers import MetadataTransformer
        return MetadataTransformer

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client
        from gcloud_bigtable.client import DATA_SCOPE

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        project = 'PROJECT'
        user_agent = 'USER_AGENT'
        client = Client(project=project, credentials=credentials,
                        user_agent=user_agent)
        transformer = self._makeOne(client)
        self.assertTrue(transformer._credentials is scoped_creds)
        self.assertEqual(transformer._user_agent, user_agent)
        self.assertEqual(credentials._called, [
            ('create_scoped', ([DATA_SCOPE],), {}),
        ])

    def test___call__(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client
        from gcloud_bigtable.client import DATA_SCOPE
        from gcloud_bigtable.client import DEFAULT_USER_AGENT

        access_token_expected = 'FOOBARBAZ'

        class _ReturnVal(object):
            access_token = access_token_expected

        scoped_creds = _MockWithAttachedMethods(_ReturnVal)
        credentials = _MockWithAttachedMethods(scoped_creds)
        project = 'PROJECT'
        client = Client(project=project, credentials=credentials)

        transformer = self._makeOne(client)
        result = transformer(None)
        self.assertEqual(
            result,
            [
                ('Authorization', 'Bearer ' + access_token_expected),
                ('User-agent', DEFAULT_USER_AGENT),
            ])
        self.assertEqual(credentials._called, [
            ('create_scoped', ([DATA_SCOPE],), {}),
        ])
        self.assertEqual(scoped_creds._called, [('get_access_token', (), {})])


class Test_get_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable._helpers import get_certs
        return get_certs()

    def test_it(self):
        import tempfile
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        # Just write to a mock file.
        filename = tempfile.mktemp()
        contents = b'FOOBARBAZ'
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)

        with _Monkey(MUT, SSL_CERT_FILE=filename):
            result = self._callFUT()

        self.assertEqual(result, contents)


class Test_make_stub(unittest2.TestCase):

    def _callFUT(self, credentials, stub_factory, host, port):
        from gcloud_bigtable._helpers import make_stub
        return make_stub(credentials, stub_factory, host, port)

    def test_it(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        mock_result = object()
        custom_factory = _MockCalled(mock_result)
        transformed = object()
        transformer = _MockCalled(transformed)

        client_creds = object()
        channel = object()
        implementations_mod = _MockWithAttachedMethods(client_creds, channel)

        host = 'HOST'
        port = 1025
        certs = 'FOOBAR'
        client = _MockWithAttachedMethods()
        with _Monkey(MUT, get_certs=lambda: certs,
                     implementations=implementations_mod,
                     MetadataTransformer=transformer):
            result = self._callFUT(client, custom_factory, host, port)

        self.assertTrue(result is mock_result)
        custom_factory.check_called(
            self,
            [(channel,)],
            [{
                'metadata_transformer': transformed,
            }],
        )
        transformer.check_called(self, [(client,)])
        self.assertEqual(client._called, [])
        # Check what was called on the module.
        self.assertEqual(implementations_mod._called, [
            ('ssl_client_credentials', (certs,),
             {'private_key': None, 'certificate_chain': None}),
            ('secure_channel', (host, port, client_creds), {}),
        ])
