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

from gcloud_bigtable.client import Client


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
EXISTING_CLUSTERS = []
CREDENTIALS = GoogleCredentials.get_application_default()
CLIENT = Client(CREDENTIALS, project_id=PROJECT_ID)


def setUpModule():
    clusters, failed_zones = CLIENT.list_clusters()

    if len(failed_zones) != 0:
        raise ValueError('List clusters failed in module set up.')

    EXISTING_CLUSTERS[:] = clusters


class TestClusterAdminAPI(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a cluster which will remain throughout tests.
        cluster = CLIENT.cluster(TEST_ZONE, TEST_CLUSTER_ID,
                                 display_name=TEST_CLUSTER_ID)
        cluster.create()
        cls._cluster = cluster

    @classmethod
    def tearDownClass(cls):
        cls._cluster.delete()
        del cls._cluster

    def setUp(self):
        self.clusters_to_delete = []

    def tearDown(self):
        for cluster in self.clusters_to_delete:
            cluster.delete()

    def test_list_zones(self):
        zones = CLIENT.list_zones()
        self.assertEqual(sorted(zones), list(EXPECTED_ZONES))

    def test_list_clusters(self):
        clusters, failed_zones = CLIENT.list_clusters()
        self.assertEqual(failed_zones, [])
        # We have added one new cluster in `setUpClass`.
        self.assertEqual(len(clusters), len(EXISTING_CLUSTERS) + 1)
        for cluster in clusters:
            cluster_existence = (cluster in EXISTING_CLUSTERS or
                                 cluster == self._cluster)
            self.assertTrue(cluster_existence)

    def test_reload(self):
        # Use same arguments as self._cluster (created in `setUpClass`).
        cluster = CLIENT.cluster(TEST_ZONE, TEST_CLUSTER_ID)
        # Make sure metadata unset before reloading.
        cluster.display_name = None
        cluster.serve_nodes = None

        cluster.reload()
        self.assertEqual(cluster.display_name, self._cluster.display_name)
        self.assertEqual(cluster.serve_nodes, self._cluster.serve_nodes)

    def test_create_cluster(self):
        cluster_id = '%s-%d' % (TEST_CLUSTER_ID, 1000 * time.time())
        cluster = CLIENT.cluster(TEST_ZONE, cluster_id)
        cluster.create()
        # Make sure this cluster gets deleted after the test case.
        self.clusters_to_delete.append(cluster)

        # We want to make sure the operation completes.
        time.sleep(2)
        self.assertTrue(cluster.operation_finished())

        # Create a new cluster instance and make sure it is the same.
        cluster_alt = CLIENT.cluster(TEST_ZONE, cluster_id)
        cluster_alt.reload()

        self.assertEqual(cluster, cluster_alt)
        self.assertEqual(cluster.display_name, cluster_alt.display_name)
        self.assertEqual(cluster.serve_nodes, cluster_alt.serve_nodes)

    def test_update(self):
        curr_display_name = self._cluster.display_name
        self._cluster.display_name = 'Foo Bar Baz'
        self._cluster.update()

        # We want to make sure the operation completes.
        time.sleep(2)
        self.assertTrue(self._cluster.operation_finished())

        # Create a new cluster instance and make sure it is the same.
        cluster_alt = CLIENT.cluster(TEST_ZONE, TEST_CLUSTER_ID)
        self.assertNotEqual(cluster_alt.display_name,
                            self._cluster.display_name)
        cluster_alt.reload()
        self.assertEqual(cluster_alt.display_name,
                         self._cluster.display_name)

        # Make sure to put the cluster back the way it was for the
        # other test cases.
        self._cluster.display_name = curr_display_name
        self._cluster.update()

        # We want to make sure the operation completes.
        time.sleep(2)
        self.assertTrue(self._cluster.operation_finished())
