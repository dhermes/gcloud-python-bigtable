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

import time
import unittest2

from happybase import Connection


NOW_MILLIS = int(1000 * time.time())
TABLE_NAME = 'table-name'
ALT_TABLE_NAME = 'other-table'
TTL_FOR_TEST = 3
COL_FAM1 = 'cf1'
COL_FAM2 = 'cf2'
COL_FAM3 = 'cf3'
FAMILIES = {
    COL_FAM1: {'max_versions': 10},
    COL_FAM2: {'max_versions': 1, 'time_to_live': TTL_FOR_TEST},
    COL_FAM3: {},  # use defaults
}
ROW_KEY1 = 'row-key1'
ROW_KEY2 = 'row-key2'
COL1 = COL_FAM1 + ':qual1'
COL2 = COL_FAM1 + ':qual2'
COL3 = COL_FAM2 + ':qual1'
COL4 = COL_FAM3 + ':qual3'


class Config(object):
    """Simple namespace for holding test globals."""

    connection = None
    table = None


# BEGIN: Backend dependent values.
USING_HBASE = True


def get_connection():
    if Config.connection is None:
        Config.connection = Connection()
    return Config.connection
#   END: Backend dependent values.


def get_table():
    if Config.table is None:
        connection = get_connection()
        Config.table = connection.table(TABLE_NAME)
    return Config.table


def setUpModule():
    if Config.table is None:
        connection = get_connection()
        if TABLE_NAME not in connection.tables():
            connection.create_table(TABLE_NAME, FAMILIES)


def tearDownModule():
    # connection.delete_table(TABLE_NAME, disable=True)
    pass


class TestConnection(unittest2.TestCase):

    @unittest2.skip('Creation hangs on some runs, bad for rapid development')
    def test_create_and_delete_table(self):
        connection = get_connection()

        self.assertFalse(ALT_TABLE_NAME in connection.tables())
        connection.create_table(ALT_TABLE_NAME, {COL_FAM1: {}})
        self.assertTrue(ALT_TABLE_NAME in connection.tables())
        connection.delete_table(ALT_TABLE_NAME, disable=True)
        self.assertFalse(ALT_TABLE_NAME in connection.tables())

    def test_families(self):
        table = get_table()
        families = table.families()

        self.assertEqual(set(families.keys()), set(FAMILIES.keys()))
        for col_fam, settings in FAMILIES.items():
            retrieved = families[col_fam]
            for key, value in settings.items():
                if key == 'time_to_live' and USING_HBASE:
                    # The Thrift API fails to retrieve the TTL for some reason.
                    continue
                self.assertEqual(retrieved[key], value)


class TestTable(unittest2.TestCase):

    def setUp(self):
        self.rows_to_delete = []

    def tearDown(self):
        table = get_table()
        for row_key in self.rows_to_delete:
            table.delete(row_key)

    def test_empty_rows(self):
        table = get_table()
        row1 = table.row(ROW_KEY1)
        row2 = table.row(ROW_KEY2)

        self.assertEqual(row1, {})
        self.assertEqual(row2, {})

    def test_put(self):
        table = get_table()
        value1 = 'value1'
        value2 = 'value2'
        row1_data = {COL1: value1, COL2: value2}

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, row1_data)

        row1 = table.row(ROW_KEY1)
        self.assertEqual(row1, row1_data)

        # Check again, but this time with timestamps.
        row1 = table.row(ROW_KEY1, include_timestamp=True)
        timestamp1 = row1[COL1][1]
        timestamp2 = row1[COL2][1]
        self.assertEqual(timestamp1, timestamp2)

        row1_data_with_timestamps = {COL1: (value1, timestamp1),
                                     COL2: (value2, timestamp2)}
        self.assertEqual(row1, row1_data_with_timestamps)

    @unittest2.skipIf(USING_HBASE, 'HBase fails to write with a timestamp')
    def test_put_with_timestamp(self):
        table = get_table()
        value1 = 'value1'
        value2 = 'value2'
        row1_data = {COL1: value1, COL2: value2}
        ts = NOW_MILLIS

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, row1_data, timestamp=ts)

        # Check again, but this time with timestamps.
        row1 = table.row(ROW_KEY1, include_timestamp=True)
        row1_data_with_timestamps = {COL1: (value1, ts),
                                     COL2: (value2, ts)}
        self.assertEqual(row1, row1_data_with_timestamps)

    def test_put_versions_restricted(self):
        table = get_table()
        families = table.families()

        chosen_fam = COL_FAM2
        self.assertEqual(families[chosen_fam]['max_versions'], 1)
        chosen_col = COL3
        self.assertTrue(chosen_col.startswith(chosen_fam + ':'))

        value1 = 'value1'
        value2 = 'value2'

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, {chosen_col: value1})

        all_values_before = table.cells(ROW_KEY1, chosen_col, versions=2)
        self.assertEqual(all_values_before, [value1])

        # Putting another value should evict the first one.
        table.put(ROW_KEY1, {chosen_col: value2})
        all_values_after = table.cells(ROW_KEY1, chosen_col, versions=2)
        self.assertEqual(all_values_after, [value2])

    @unittest2.skip('Sleeping 3.5 seconds is bad for rapid development')
    def test_put_ttl_eviction(self):
        table = get_table()
        # The Thrift API fails to retrieve the TTL for some reason.
        if USING_HBASE:
            families = FAMILIES
        else:
            families = table.families()

        cell_tll = TTL_FOR_TEST
        chosen_fam = COL_FAM2
        self.assertEqual(families[chosen_fam]['time_to_live'], cell_tll)
        chosen_col = COL3
        self.assertTrue(chosen_col.startswith(chosen_fam + ':'))

        value1 = 'value1'

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, {chosen_col: value1})

        all_values_before = table.cells(ROW_KEY1, chosen_col)
        self.assertEqual(all_values_before, [value1])

        # Make sure we don't sleep for a problematic length.
        self.assertTrue(cell_tll < 10)
        # Wait for time-to-live eviction to occur.
        time.sleep(cell_tll + 0.5)
        all_values_after = table.cells(ROW_KEY1, chosen_col)
        self.assertEqual(all_values_after, [])

    def test_cells(self):
        table = get_table()
        value1 = 'value1'
        value2 = 'value2'
        value3 = 'value3'

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, {COL1: value1})
        table.put(ROW_KEY1, {COL1: value2})
        table.put(ROW_KEY1, {COL1: value3})

        # Check with no extra arguments.
        all_values = table.cells(ROW_KEY1, COL1)
        self.assertEqual(all_values, [value3, value2, value1])

        # Check the timestamp on all the cells.
        all_cells = table.cells(ROW_KEY1, COL1, include_timestamp=True)
        self.assertEqual(len(all_cells), 3)

        ts3 = all_cells[0][1]
        ts2 = all_cells[1][1]
        ts1 = all_cells[2][1]
        self.assertEqual(all_cells,
                         [(value3, ts3), (value2, ts2), (value1, ts1)])

        # Limit to the two latest cells.
        latest_two = table.cells(ROW_KEY1, COL1, include_timestamp=True,
                                 versions=2)
        self.assertEqual(latest_two, [(value3, ts3), (value2, ts2)])

        # Limit to cells before the 2nd timestamp (inclusive).
        first_two = table.cells(ROW_KEY1, COL1, include_timestamp=True,
                                timestamp=ts2 + 1)
        self.assertEqual(first_two, [(value2, ts2), (value1, ts1)])

        # Limit to cells before the 2nd timestamp (exclusive).
        first_cell = table.cells(ROW_KEY1, COL1, include_timestamp=True,
                                 timestamp=ts2)
        self.assertEqual(first_cell, [(value1, ts1)])

    def test_row_with_columns(self):
        table = get_table()
        value1 = 'value1'
        value2 = 'value2'
        value3 = 'value3'
        value4 = 'value4'
        row1_data = {
            COL1: value1,
            COL2: value2,
            COL3: value3,
            COL4: value4,
        }

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, row1_data)

        # Make sure the vanilla write succeeded.
        row1 = table.row(ROW_KEY1)
        self.assertEqual(row1, row1_data)

        # Pick out specific columns.
        row1_diff_fams = table.row(ROW_KEY1, columns=[COL1, COL4])
        self.assertEqual(row1_diff_fams, {COL1: value1, COL4: value4})
        row1_single_col = table.row(ROW_KEY1, columns=[COL3])
        self.assertEqual(row1_single_col, {COL3: value3})
        row1_col_fam = table.row(ROW_KEY1, columns=[COL_FAM1])
        self.assertEqual(row1_col_fam, {COL1: value1, COL2: value2})
        row1_fam_qual_overlap1 = table.row(ROW_KEY1, columns=[COL1, COL_FAM1])
        self.assertEqual(row1_fam_qual_overlap1, {COL1: value1, COL2: value2})
        # NOTE: This behavior seems to be "incorrect" but that is how
        #       happybase works.
        row1_fam_qual_overlap2 = table.row(ROW_KEY1, columns=[COL_FAM1, COL1])
        self.assertEqual(row1_fam_qual_overlap2, {COL1: value1})
        row1_multiple_col_fams = table.row(ROW_KEY1,
                                           columns=[COL_FAM1, COL_FAM2])
        self.assertEqual(row1_multiple_col_fams,
                         {COL1: value1, COL2: value2, COL3: value3})

    def test_row_with_timestamp(self):
        table = get_table()
        value1 = 'value1'
        value2 = 'value2'
        value3 = 'value3'

        # Need to clean-up row1 after.
        self.rows_to_delete.append(ROW_KEY1)
        table.put(ROW_KEY1, {COL1: value1})
        table.put(ROW_KEY1, {COL2: value2})
        table.put(ROW_KEY1, {COL3: value3})

        # Make sure the vanilla write succeeded.
        row1 = table.row(ROW_KEY1, include_timestamp=True)
        ts1 = row1[COL1][1]
        ts2 = row1[COL2][1]
        ts3 = row1[COL3][1]

        expected_row = {
            COL1: (value1, ts1),
            COL2: (value2, ts2),
            COL3: (value3, ts3),
        }
        self.assertEqual(row1, expected_row)

        # Make sure the timestamps are (strictly) ascending.
        self.assertTrue(ts1 < ts2 < ts3)

        # Use timestamps to retrieve row.
        first_two = table.row(ROW_KEY1, timestamp=ts2 + 1,
                              include_timestamp=True)
        self.assertEqual(first_two, {
            COL1: (value1, ts1),
            COL2: (value2, ts2),
        })
        first_one = table.row(ROW_KEY1, timestamp=ts2,
                              include_timestamp=True)
        self.assertEqual(first_one, {
            COL1: (value1, ts1),
        })
