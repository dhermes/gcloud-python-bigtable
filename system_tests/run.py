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

import datetime
import operator
import os
import pytz
import time
import unittest2

from oauth2client.client import GoogleCredentials

from gcloud_bigtable.client import Client


PROJECT_ID = os.getenv('GCLOUD_TESTS_PROJECT_ID')
CENTRAL_1C_ZONE = 'us-central1-c'
CLUSTER_ID = 'gcloud-python'
SERVE_NODES = 3
TABLE_ID = 'gcloud-python-test-table'
COLUMN_FAMILY_ID1 = u'col-fam-id1'
COLUMN_FAMILY_ID2 = u'col-fam-id2'
ROW_KEY = b'row-key'
EXPECTED_ZONES = (
    'asia-east1-b',
    'europe-west1-c',
    'us-central1-b',
    CENTRAL_1C_ZONE,
)
EXISTING_CLUSTERS = []
CREDENTIALS = GoogleCredentials.get_application_default()
CLIENT = Client(CREDENTIALS, project_id=PROJECT_ID, admin=True)
CLUSTER = CLIENT.cluster(CENTRAL_1C_ZONE, CLUSTER_ID,
                         display_name=CLUSTER_ID)


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
        cluster = CLIENT.cluster(CENTRAL_1C_ZONE, CLUSTER_ID)
        # Make sure metadata unset before reloading.
        cluster.display_name = None
        cluster.serve_nodes = None

        cluster.reload()
        self.assertEqual(cluster.display_name, CLUSTER.display_name)
        self.assertEqual(cluster.serve_nodes, CLUSTER.serve_nodes)

    def test_create_cluster(self):
        cluster_id = '%s-%d' % (CLUSTER_ID, 1000 * time.time())
        cluster = CLIENT.cluster(CENTRAL_1C_ZONE, cluster_id)
        cluster.create()
        # Make sure this cluster gets deleted after the test case.
        self.clusters_to_delete.append(cluster)

        # We want to make sure the operation completes.
        time.sleep(2)
        self.assertTrue(cluster.operation_finished())

        # Create a new cluster instance and make sure it is the same.
        cluster_alt = CLIENT.cluster(CENTRAL_1C_ZONE, cluster_id)
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
        cluster_alt = CLIENT.cluster(CENTRAL_1C_ZONE, CLUSTER_ID)
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
        self._table = CLUSTER.table(TABLE_ID)
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
        name_attr = operator.attrgetter('name')
        expected_tables = sorted([temp_table, self._table], key=name_attr)

        # Then query for the tables in the cluster and sort them by
        # name as well.
        tables = CLUSTER.list_tables()
        sorted_tables = sorted(tables, key=name_attr)
        self.assertEqual(sorted_tables, expected_tables)

    def test_create_column_family(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule
        temp_table_id = 'foo-bar-baz-table'
        temp_table = CLUSTER.table(temp_table_id)
        temp_table.create()
        self.tables_to_delete.append(temp_table)

        self.assertEqual(temp_table.list_column_families(), {})
        gc_rule = GarbageCollectionRule(max_num_versions=1)
        column_family = temp_table.column_family(COLUMN_FAMILY_ID1,
                                                 gc_rule=gc_rule)
        column_family.create()

        col_fams = temp_table.list_column_families()

        self.assertEqual(len(col_fams), 1)
        retrieved_col_fam = col_fams[COLUMN_FAMILY_ID1]
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
        column_family = temp_table.column_family(COLUMN_FAMILY_ID1)
        column_family.create()

        # Make sure the family is there before deleting it.
        col_fams = temp_table.list_column_families()
        self.assertEqual(list(col_fams.keys()), [COLUMN_FAMILY_ID1])

        column_family.delete()
        # Make sure we have successfully deleted it.
        self.assertEqual(temp_table.list_column_families(), {})


class TestDataAPI(unittest2.TestCase):

    @classmethod
    def setUpClass(self):
        self._table = table = CLUSTER.table(TABLE_ID)
        table.create()
        table.column_family(COLUMN_FAMILY_ID1).create()
        table.column_family(COLUMN_FAMILY_ID2).create()

    @classmethod
    def tearDownClass(self):
        # Will also delete any data contained in the table.
        self._table.delete()

    def test_read_row(self):
        from gcloud_bigtable._helpers import _microseconds_to_timestamp
        from gcloud_bigtable._helpers import _timestamp_to_microseconds
        from gcloud_bigtable.row_data import Cell

        col_name1 = b'col-name1'
        col_name2 = b'col-name2'
        col_name3 = b'col-name3-but-other-fam'
        cell_val1 = b'cell-val'
        cell_val2 = b'cell-val-newer'
        cell_val3 = b'altcol-cell-val'
        cell_val4 = b'foo'

        timestamp1 = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        # Must be millisecond granularity.
        timestamp1 = _microseconds_to_timestamp(
            _timestamp_to_microseconds(timestamp1))
        # 1000 microseconds is a millisecond
        timestamp2 = timestamp1 + datetime.timedelta(microseconds=1000)
        timestamp3 = timestamp1 + datetime.timedelta(microseconds=2000)
        timestamp4 = timestamp1 + datetime.timedelta(microseconds=3000)
        row = self._table.row(ROW_KEY)
        row.set_cell(COLUMN_FAMILY_ID1, col_name1, cell_val1,
                     timestamp=timestamp1)
        row.set_cell(COLUMN_FAMILY_ID1, col_name1, cell_val2,
                     timestamp=timestamp2)
        row.set_cell(COLUMN_FAMILY_ID1, col_name2, cell_val3,
                     timestamp=timestamp3)
        row.set_cell(COLUMN_FAMILY_ID2, col_name3, cell_val4,
                     timestamp=timestamp4)
        row.commit()

        # Create the cells we will check.
        cell1 = Cell(cell_val1, timestamp1)
        cell2 = Cell(cell_val2, timestamp2)
        cell3 = Cell(cell_val3, timestamp3)
        cell4 = Cell(cell_val4, timestamp4)

        # Read back the contents of the row.
        partial_row_data = self._table.read_row(ROW_KEY)
        self.assertTrue(partial_row_data.committed)
        self.assertEqual(partial_row_data.row_key, ROW_KEY)

        # Check the cells match. First by the column families.
        row_contents = partial_row_data.cells
        self.assertEqual(set(row_contents.keys()),
                         set([COLUMN_FAMILY_ID1, COLUMN_FAMILY_ID2]))

        # Check the first column family.
        family1 = row_contents[COLUMN_FAMILY_ID1]
        self.assertEqual(set(family1.keys()), set([col_name1, col_name2]))
        col1 = family1[col_name1]
        expected_col1 = [cell1, cell2]
        ts_attr = operator.attrgetter('timestamp')
        self.assertEqual(sorted(col1, key=ts_attr),
                         sorted(expected_col1, key=ts_attr))
        col2 = family1[col_name2]
        self.assertEqual(col2, [cell3])

        # Check the second column family.
        family2 = row_contents[COLUMN_FAMILY_ID2]
        self.assertEqual(family2, {col_name3: [cell4]})
