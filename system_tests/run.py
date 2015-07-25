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

from __future__ import print_function

import os
import time
import unittest2

from oauth2client.client import GoogleCredentials

from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages_pb2)
from gcloud_bigtable.client import Client
from gcloud_bigtable.cluster import Cluster
from gcloud_bigtable.cluster_connection import ClusterConnection


PROJECT_ID = os.getenv('GCLOUD_TESTS_PROJECT_ID')
TEST_ZONE = 'us-central1-c'
TEST_CLUSTER_ID = 'gcloud-python'
TEST_SERVE_NODES = 3
EXPECTED_ZONES = (
    'asia-east1-b',
    'europe-west1-c',
    'us-central1-b',
    TEST_ZONE,
)
EXISTING_CLUSTER_NAMES = []


def setUpModule():
    credentials = GoogleCredentials.get_application_default()
    connection = ClusterConnection(credentials)
    result_pb = connection.list_clusters(PROJECT_ID)

    if len(result_pb.failed_zones) != 0:
        raise ValueError('List clusters failed in module set up.')

    for cluster_pb in result_pb.clusters:
        EXISTING_CLUSTER_NAMES.append(cluster_pb.name)


class TestClusterAdminAPI(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        # By using no credentials argument in the constructor, we get the
        # application default credentials.
        cls._cluster = Cluster(PROJECT_ID, TEST_ZONE, TEST_CLUSTER_ID)
        cls._connection = cls._cluster._cluster_conn
        cls._cluster.create(display_name=TEST_CLUSTER_ID)

    @classmethod
    def tearDownClass(cls):
        cls._cluster.delete()

    def setUp(self):
        self.clusters_to_delete = []

    def tearDown(self):
        for cluster in self.clusters_to_delete:
            cluster.delete()

    def test_list_zones(self):
        result_pb = self._connection.list_zones(PROJECT_ID)
        self.assertTrue(isinstance(result_pb, messages_pb2.ListZonesResponse))

        self.assertEqual(len(result_pb.zones), 4)
        all_zones = sorted(result_pb.zones, key=lambda zone: zone.name)

        OK_STATUS = 1
        for curr_zone, expected_name in zip(all_zones, EXPECTED_ZONES):
            self.assertEqual(
                curr_zone.name,
                'projects/%s/zones/%s' % (PROJECT_ID, expected_name))
            self.assertEqual(curr_zone.display_name, expected_name)
            self.assertEqual(curr_zone.status, OK_STATUS)

    def test_reload(self):
        credentials = self._cluster._credentials
        # Use same arguments as self._cluster (created in `setUpClass`).
        cluster = Cluster(PROJECT_ID, TEST_ZONE, TEST_CLUSTER_ID,
                          credentials=credentials)
        # Make sure metadata unset before reloading.
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        cluster.reload()
        self.assertEqual(cluster.display_name, self._cluster.display_name)
        self.assertEqual(cluster.serve_nodes, self._cluster.serve_nodes)

    def _assert_test_cluster(self, cluster_pb, cluster_id=TEST_CLUSTER_ID,
                             display_name=None):
        fields_set = sorted([field.name
                             for field in cluster_pb._fields.keys()])
        self.assertEqual(fields_set, [
            'default_storage_type',
            'display_name',
            'name',
            'serve_nodes',
        ])
        self.assertEqual(cluster_pb.serve_nodes, TEST_SERVE_NODES)
        if display_name is None:
            self.assertEqual(cluster_pb.display_name, cluster_id)
        else:
            self.assertEqual(cluster_pb.display_name, display_name)
        full_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, TEST_ZONE, cluster_id)
        self.assertEqual(cluster_pb.name, full_name)
        self.assertEqual(cluster_pb.default_storage_type,
                         data_pb2.STORAGE_SSD)

    def test_create_cluster(self):
        cluster_id = '%s-%d' % (TEST_CLUSTER_ID, 1000 * time.time())
        cluster = Cluster(PROJECT_ID, TEST_ZONE, cluster_id)
        # Before creation, these are unset.
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)
        cluster.create(display_name=cluster_id)
        # After creation, they will be unset.
        self.assertEqual(cluster.display_name, cluster_id)
        self.assertEqual(cluster.serve_nodes, 3)

        result_pb = cluster._cluster_conn.get_cluster(PROJECT_ID, TEST_ZONE,
                                                      cluster_id)
        self._assert_test_cluster(result_pb, cluster_id=cluster_id)
        # Make sure this cluster gets deleted after the test case.
        self.clusters_to_delete.append(cluster)

    def test_get_cluster(self):
        # We assume this cluster exists since it was created in `setUpClass`.
        result_pb = self._connection.get_cluster(PROJECT_ID, TEST_ZONE,
                                                 TEST_CLUSTER_ID)
        self._assert_test_cluster(result_pb)

    def test_list_clusters(self):
        result_pb = self._connection.list_clusters(PROJECT_ID)

        self.assertEqual(list(result_pb.failed_zones), [])
        # We just want to look at the clusters that did not exist before this
        # test module began running.
        test_clusters = [cluster_pb for cluster_pb in result_pb.clusters
                         if cluster_pb.name not in EXISTING_CLUSTER_NAMES]
        # We assume all test cases have cleaned up created clusters and
        # the only one remaining is the one from `setUpClass`.
        cluster, = test_clusters
        self._assert_test_cluster(cluster)

    def test_update_cluster(self):
        curr_display_name = self._cluster.display_name
        temp_display_name = 'Foo Bar Baz'
        self.assertNotEqual(curr_display_name, temp_display_name)
        self._cluster.update(display_name=temp_display_name)
        # Make sure the display name has changed after the request.
        self.assertEqual(self._cluster.display_name, temp_display_name)

        result_pb = self._connection.get_cluster(
            PROJECT_ID, TEST_ZONE, TEST_CLUSTER_ID)
        self._assert_test_cluster(result_pb, display_name=temp_display_name)

        # Restore the original state of the cluster.
        self._cluster.update(display_name=curr_display_name)


class TestClient(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        credentials = GoogleCredentials.get_application_default()
        cls._client = Client(credentials, project_id=PROJECT_ID)

    @classmethod
    def tearDownClass(cls):
        del cls._client

    def test_list_zones(self):
        zones = self._client.list_zones()
        self.assertEqual(sorted(zones), list(EXPECTED_ZONES))

    def test_list_clusters(self):
        clusters, failed_zones = self._client.list_clusters()
        self.assertEqual(failed_zones, [])
        self.assertEqual(len(clusters), len(EXISTING_CLUSTER_NAMES))
        for cluster in clusters:
            self.assertTrue(cluster.name in EXISTING_CLUSTER_NAMES)
