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


PROJECT_ID = 'PROJECT_ID'
ZONE = 'ZONE'
CLUSTER_ID = 'CLUSTER_ID'


class TestCluster(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.cluster_standalone import Cluster
        return Cluster

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        client = object()
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertTrue(cluster._client is client)

    def test_client_getter(self):
        client = object()
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertTrue(cluster.client is client)

    def test_project_id_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=PROJECT_ID)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.project_id, PROJECT_ID)
