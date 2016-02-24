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


class TestMetadataPlugin(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable._helpers import MetadataPlugin
        return MetadataPlugin

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
        plugin = self._makeOne(client)
        self.assertTrue(plugin._credentials is scoped_creds)
        self.assertEqual(plugin._user_agent, user_agent)
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

        callback_args = []

        def callback(value1, value2):
            callback_args.append((value1, value2))

        scoped_creds = _MockWithAttachedMethods(_ReturnVal)
        credentials = _MockWithAttachedMethods(scoped_creds)
        project = 'PROJECT'
        client = Client(project=project, credentials=credentials)

        plugin = self._makeOne(client)
        result = plugin(None, callback)
        self.assertEqual(result, None)
        cb_headers = [
            ('Authorization', 'Bearer ' + access_token_expected),
            ('User-agent', DEFAULT_USER_AGENT),
        ]
        self.assertEqual(callback_args, [(cb_headers, None)])
        self.assertEqual(credentials._called, [
            ('create_scoped', ([DATA_SCOPE],), {}),
        ])
        self.assertEqual(scoped_creds._called, [('get_access_token', (), {})])


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
        mock_plugin = object()
        plugin = _MockCalled(mock_plugin)

        ssl_creds = object()
        metadata_creds = object()
        composite_creds = object()
        channel = object()
        implementations_mod = _MockWithAttachedMethods(
            ssl_creds, metadata_creds, composite_creds, channel)

        host = 'HOST'
        port = 1025
        client = _MockWithAttachedMethods()
        with _Monkey(MUT, implementations=implementations_mod,
                     MetadataPlugin=plugin):
            result = self._callFUT(client, custom_factory, host, port)

        self.assertTrue(result is mock_result)
        custom_factory.check_called(
            self,
            [(channel,)],
            [{}],
        )
        plugin.check_called(self, [(client,)])
        self.assertEqual(client._called, [])
        # Check what was called on the module.
        self.assertEqual(implementations_mod._called, [
            ('ssl_channel_credentials', (None, None, None), {}),
            ('metadata_call_credentials', (mock_plugin,),
             {'name': 'google_creds'}),
            ('composite_channel_credentials',
             (ssl_creds, metadata_creds), {}),
            ('secure_channel', (host, port, composite_creds), {}),
        ])
