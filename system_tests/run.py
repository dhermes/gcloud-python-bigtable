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


class TestClient(unittest2.TestCase):

    def test_list_zones(self):
        zones = CLIENT.list_zones()
        self.assertEqual(sorted(zones), list(EXPECTED_ZONES))

    def test_list_clusters(self):
        clusters, failed_zones = CLIENT.list_clusters()
        self.assertEqual(failed_zones, [])
        self.assertEqual(len(clusters), len(EXISTING_CLUSTERS))
        for cluster in clusters:
            self.assertTrue(cluster in EXISTING_CLUSTERS)
