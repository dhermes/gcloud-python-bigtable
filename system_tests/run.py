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

from gcloud_bigtable._helpers import _microseconds_to_timestamp
from gcloud_bigtable._helpers import _timestamp_to_microseconds
from gcloud_bigtable.client import Client
from gcloud_bigtable.column_family import GarbageCollectionRule
from gcloud_bigtable.row_data import Cell
from gcloud_bigtable.row_data import PartialRowData


PROJECT_ID = os.getenv('GCLOUD_TESTS_PROJECT_ID')
CENTRAL_1C_ZONE = 'us-central1-c'
NOW_MILLIS = int(1000 * time.time())
CLUSTER_ID = 'gcloud-python-%d' % (NOW_MILLIS,)
SERVE_NODES = 3
TABLE_ID = 'gcloud-python-test-table'
COLUMN_FAMILY_ID1 = u'col-fam-id1'
COLUMN_FAMILY_ID2 = u'col-fam-id2'
COL_NAME1 = b'col-name1'
COL_NAME2 = b'col-name2'
COL_NAME3 = b'col-name3-but-other-fam'
CELL_VAL1 = b'cell-val'
CELL_VAL2 = b'cell-val-newer'
CELL_VAL3 = b'altcol-cell-val'
CELL_VAL4 = b'foo'
ROW_KEY = b'row-key'
ROW_KEY_ALT = b'row-key-alt'
EXPECTED_ZONES = (
    'asia-east1-b',
    'europe-west1-c',
    'us-central1-b',
    CENTRAL_1C_ZONE,
)
EXISTING_CLUSTERS = []
CREDENTIALS = GoogleCredentials.get_application_default()
CLIENT = Client(project=PROJECT_ID, credentials=CREDENTIALS, admin=True)
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
        cluster_id = '%s-a' % (CLUSTER_ID,)
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
        self.assertEqual(retrieved_col_fam.gc_rule, gc_rule)

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

    def setUp(self):
        self.rows_to_delete = []

    def tearDown(self):
        for row in self.rows_to_delete:
            row.clear_mutations()
            row.delete()
            row.commit()

    def _write_to_row(self, row1=None, row2=None, row3=None, row4=None):
        timestamp1 = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        # Must be millisecond granularity.
        timestamp1 = _microseconds_to_timestamp(
            _timestamp_to_microseconds(timestamp1))
        # 1000 microseconds is a millisecond
        timestamp2 = timestamp1 + datetime.timedelta(microseconds=1000)
        timestamp3 = timestamp1 + datetime.timedelta(microseconds=2000)
        timestamp4 = timestamp1 + datetime.timedelta(microseconds=3000)
        if row1 is not None:
            row1.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL1,
                          timestamp=timestamp1)
        if row2 is not None:
            row2.set_cell(COLUMN_FAMILY_ID1, COL_NAME1, CELL_VAL2,
                          timestamp=timestamp2)
        if row3 is not None:
            row3.set_cell(COLUMN_FAMILY_ID1, COL_NAME2, CELL_VAL3,
                          timestamp=timestamp3)
        if row4 is not None:
            row4.set_cell(COLUMN_FAMILY_ID2, COL_NAME3, CELL_VAL4,
                          timestamp=timestamp4)

        # Create the cells we will check.
        cell1 = Cell(CELL_VAL1, timestamp1)
        cell2 = Cell(CELL_VAL2, timestamp2)
        cell3 = Cell(CELL_VAL3, timestamp3)
        cell4 = Cell(CELL_VAL4, timestamp4)
        return cell1, cell2, cell3, cell4

    def test_read_row(self):
        row = self._table.row(ROW_KEY)
        self.rows_to_delete.append(row)

        cell1, cell2, cell3, cell4 = self._write_to_row(row, row, row, row)
        row.commit()

        # Read back the contents of the row.
        partial_row_data = self._table.read_row(ROW_KEY)
        self.assertTrue(partial_row_data.committed)
        self.assertEqual(partial_row_data.row_key, ROW_KEY)

        # Check the cells match.
        ts_attr = operator.attrgetter('timestamp')
        expected_row_contents = {
            COLUMN_FAMILY_ID1: {
                COL_NAME1: sorted([cell1, cell2], key=ts_attr, reverse=True),
                COL_NAME2: [cell3],
            },
            COLUMN_FAMILY_ID2: {
                COL_NAME3: [cell4],
            },
        }
        self.assertEqual(partial_row_data.cells, expected_row_contents)

    def test_read_rows(self):
        row = self._table.row(ROW_KEY)
        row_alt = self._table.row(ROW_KEY_ALT)
        self.rows_to_delete.extend([row, row_alt])

        cell1, cell2, cell3, cell4 = self._write_to_row(row, row_alt,
                                                        row, row_alt)
        row.commit()
        row_alt.commit()

        rows_data = self._table.read_rows()
        self.assertEqual(rows_data.rows, {})
        rows_data.consume_all()

        # NOTE: We should refrain from editing protected data on instances.
        #       Instead we should make the values public or provide factories
        #       for constructing objects with them.
        row_data = PartialRowData(ROW_KEY)
        row_data._chunks_encountered = True
        row_data._committed = True
        row_data._cells = {
            COLUMN_FAMILY_ID1: {
                COL_NAME1: [cell1],
                COL_NAME2: [cell3],
            },
        }

        row_alt_data = PartialRowData(ROW_KEY_ALT)
        row_alt_data._chunks_encountered = True
        row_alt_data._committed = True
        row_alt_data._cells = {
            COLUMN_FAMILY_ID1: {
                COL_NAME1: [cell2],
            },
            COLUMN_FAMILY_ID2: {
                COL_NAME3: [cell4],
            },
        }

        expected_rows = {
            ROW_KEY: row_data,
            ROW_KEY_ALT: row_alt_data,
        }
        self.assertEqual(rows_data.rows, expected_rows)
