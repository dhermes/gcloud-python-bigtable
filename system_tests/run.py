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
import unittest2

from oauth2client.client import GoogleCredentials

from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages_pb2)
from gcloud_bigtable.cluster_connection import ClusterConnection


PROJECT_ID = os.getenv('GCLOUD_TESTS_PROJECT_ID')
TEST_ZONE_NAME = 'us-central1-c'
TEST_CLUSTER_ID = 'gcloud-python-cluster'
TEST_NUMBER_OF_NODES = 3
EXPECTED_ZONES = (
    'asia-east1-b',
    'europe-west1-c',
    'us-central1-b',
    TEST_ZONE_NAME,
)


class TestClusterAdminAPI(unittest2.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._credentials = cls._get_creds()
        cls._connection = ClusterConnection(cls._credentials)

    @staticmethod
    def _get_creds():
        """Get credentials for a test.

        Currently (as of July 20, 2015) the Cluster Admin API does not
        support service accounts, so only user credentials are
        used here.
        """
        return GoogleCredentials.get_application_default()

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

    def _assert_test_cluster(self, cluster):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        fields_set = sorted([field.name
                             for field in cluster._fields.keys()])
        self.assertEqual(fields_set, [
            'default_storage_type',
            'display_name',
            'name',
            'serve_nodes',
        ])
        self.assertEqual(cluster.serve_nodes, TEST_NUMBER_OF_NODES)
        self.assertEqual(cluster.display_name, TEST_CLUSTER_ID)
        full_name = 'projects/%s/zones/%s/clusters/%s' % (
            PROJECT_ID, TEST_ZONE_NAME, TEST_CLUSTER_ID)
        self.assertEqual(cluster.name, full_name)
        self.assertEqual(cluster.default_storage_type,
                         data_pb2.STORAGE_SSD)

    @unittest2.skip('Temporarily disabling while transitioning to create')
    def test_get_cluster(self):
        result_pb = self._connection.get_cluster(PROJECT_ID, TEST_ZONE_NAME,
                                                 TEST_CLUSTER_ID)
        self._assert_test_cluster(result_pb)

    @unittest2.skip('Temporarily disabling while transitioning to create')
    def test_list_clusters(self):
        result_pb = self._connection.list_clusters(PROJECT_ID)

        self.assertEqual(list(result_pb.failed_zones), [])
        # Unpack implies a single cluster in result set.
        cluster, = result_pb.clusters
        self._assert_test_cluster(cluster)
