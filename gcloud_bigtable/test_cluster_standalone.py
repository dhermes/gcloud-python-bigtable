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


PROJECT_ID = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'


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

    def test_name_property(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=PROJECT_ID)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        self.assertEqual(cluster.name, cluster_name)

    def test_from_pb_success(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=PROJECT_ID)

        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        cluster = klass.from_pb(cluster_pb, client)
        self.assertTrue(isinstance(cluster, klass))
        self.assertEqual(cluster.client, client)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)

    def test_from_pb_bad_cluster_name(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)

        cluster_name = 'INCORRECT_FORMAT'
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, None)

    def test_from_pb_project_id_mistmatch(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        alt_project_id = 'ALT_PROJECT_ID'
        client = Client(credentials, project_id=alt_project_id)

        self.assertNotEqual(PROJECT_ID, alt_project_id)

        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, client)
