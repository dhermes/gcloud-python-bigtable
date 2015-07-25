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


class TestClient(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.client import Client
        return Client

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _constructor_test_helper(self, expected_scopes,
                                 admin=False, read_only=False):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = self._makeOne(credentials, admin=admin, read_only=read_only)

        self.assertTrue(client._credentials is scoped_creds)
        self.assertEqual(credentials._called, [
            ('create_scoped', (expected_scopes,), {}),
        ])

    def test_constructor_default(self):
        from gcloud_bigtable import client as MUT
        expected_scopes = [MUT.DATA_SCOPE]
        self._constructor_test_helper(expected_scopes)

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
