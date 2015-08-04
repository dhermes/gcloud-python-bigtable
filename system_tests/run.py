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
TEST_TABLE_ID = 'gcloud-python-test-table'
EXPECTED_ZONES = (
    'asia-east1-b',
    'europe-west1-c',
    'us-central1-b',
    TEST_ZONE,
)
EXISTING_CLUSTERS = []
CREDENTIALS = GoogleCredentials.get_application_default()
CLIENT = Client(CREDENTIALS, project_id=PROJECT_ID, admin=True)
CLUSTER = CLIENT.cluster(TEST_ZONE, TEST_CLUSTER_ID,
                         display_name=TEST_CLUSTER_ID)


def setUpModule():
    CLIENT.start()
    clusters, failed_zones = CLIENT.list_clusters()

    if len(failed_zones) != 0:
        raise ValueError('List clusters failed in module set up.')

    EXISTING_CLUSTERS[:] = clusters

    # After listing, create the test cluster.
    CLUSTER.create()


def tearDownModule():
    CLUSTER.delete()
    CLIENT.stop()


class TestClusterAdminAPI(unittest2.TestCase):

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
        # We have added one new cluster in `setUpModule`.
        self.assertEqual(len(clusters), len(EXISTING_CLUSTERS) + 1)
        for cluster in clusters:
            cluster_existence = (cluster in EXISTING_CLUSTERS or
                                 cluster == CLUSTER)
            self.assertTrue(cluster_existence)

    def test_reload(self):
        # Use same arguments as CLUSTER (created in `setUpModule`).
        cluster = CLIENT.cluster(TEST_ZONE, TEST_CLUSTER_ID)
        # Make sure metadata unset before reloading.
        cluster.display_name = None
        cluster.serve_nodes = None

        cluster.reload()
        self.assertEqual(cluster.display_name, CLUSTER.display_name)
        self.assertEqual(cluster.serve_nodes, CLUSTER.serve_nodes)

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
        curr_display_name = CLUSTER.display_name
        CLUSTER.display_name = 'Foo Bar Baz'
        CLUSTER.update()

        # We want to make sure the operation completes.
        time.sleep(2)
        self.assertTrue(CLUSTER.operation_finished())

        # Create a new cluster instance and make sure it is the same.
        cluster_alt = CLIENT.cluster(TEST_ZONE, TEST_CLUSTER_ID)
        self.assertNotEqual(cluster_alt.display_name,
                            CLUSTER.display_name)
        cluster_alt.reload()
        self.assertEqual(cluster_alt.display_name,
                         CLUSTER.display_name)

        # Make sure to put the cluster back the way it was for the
        # other test cases.
        CLUSTER.display_name = curr_display_name
        CLUSTER.update()

        # We want to make sure the operation completes.
        time.sleep(2)
        self.assertTrue(CLUSTER.operation_finished())


class TestTableAdminAPI(unittest2.TestCase):

    @classmethod
    def setUpClass(self):
        self._table = CLUSTER.table(TEST_TABLE_ID)
        self._table.create()

    @classmethod
    def tearDownClass(self):
        self._table.delete()

    def setUp(self):
        self.tables_to_delete = []

    def tearDown(self):
        for table in self.tables_to_delete:
            table.delete()

    def test_list_tables(self):
        # Since `CLUSTER` is newly created in `setUpModule`, the table
        # created in `setUpClass` here will be the only one.
        tables = CLUSTER.list_tables()
        self.assertEqual(tables, [self._table])

    def test_create_table(self):
        temp_table_id = 'foo-bar-baz-table'
        temp_table = CLUSTER.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        # First, create a sorted version of our expected result.
        expected_tables = sorted([temp_table, self._table],
                                 key=lambda value: value.name)

        # Then query for the tables in the cluster and sort them by
        # name as well.
        tables = CLUSTER.list_tables()
        sorted_tables = sorted(tables, key=lambda value: value.name)
        self.assertEqual(sorted_tables, expected_tables)

    def test_create_column_family(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule
        temp_table_id = 'foo-bar-baz-table'
        temp_table = CLUSTER.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        self.assertEqual(temp_table.list_column_families(), {})
        column_family_id = u'my-column'
        gc_rule = GarbageCollectionRule(max_num_versions=1)
        column_family = temp_table.column_family(column_family_id,
                                                 gc_rule=gc_rule)
        column_family.create()

        col_fams = temp_table.list_column_families()

        self.assertEqual(len(col_fams), 1)
        retrieved_col_fam = col_fams[column_family_id]
        self.assertTrue(retrieved_col_fam.table is column_family.table)
        self.assertEqual(retrieved_col_fam.column_family_id,
                         column_family.column_family_id)
        # Due to a quirk / bug in the API, the retrieved column family
        # does not include the GC rule.
        self.assertNotEqual(retrieved_col_fam.gc_rule, column_family.table)
        self.assertEqual(retrieved_col_fam.gc_rule, None)

    def test_delete_column_family(self):
        temp_table_id = 'foo-bar-baz-table'
        temp_table = CLUSTER.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        self.assertEqual(temp_table.list_column_families(), {})
        column_family_id = u'my-column'
        column_family = temp_table.column_family(column_family_id)
        column_family.create()

        # Make sure the family is there before deleting it.
        col_fams = temp_table.list_column_families()
        self.assertEqual(list(col_fams.keys()), [column_family_id])

        column_family.delete()
        # Make sure we have successfully deleted it.
        self.assertEqual(temp_table.list_column_families(), {})
