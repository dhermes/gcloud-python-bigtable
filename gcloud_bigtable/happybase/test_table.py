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


class Test_make_row(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import make_row
        return make_row(*args, **kwargs)

    def test_it(self):
        with self.assertRaises(NotImplementedError):
            self._callFUT({}, False)


class Test_make_ordered_row(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import make_ordered_row
        return make_ordered_row(*args, **kwargs)

    def test_it(self):
        with self.assertRaises(NotImplementedError):
            self._callFUT([], False)


class Test__gc_rule_to_dict(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import _gc_rule_to_dict
        return _gc_rule_to_dict(*args, **kwargs)

    def test_with_null(self):
        gc_rule = None
        result = self._callFUT(gc_rule)
        self.assertEqual(result, {})

    def test_with_max_versions(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule

        max_versions = 2
        gc_rule = GarbageCollectionRule(max_num_versions=max_versions)
        result = self._callFUT(gc_rule)
        expected_result = {'max_versions': max_versions}
        self.assertEqual(result, expected_result)

    def test_with_max_age(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule

        time_to_live = 101
        max_age = datetime.timedelta(seconds=time_to_live)
        gc_rule = GarbageCollectionRule(max_age=max_age)
        result = self._callFUT(gc_rule)
        expected_result = {'time_to_live': time_to_live}
        self.assertEqual(result, expected_result)

    def test_with_non_gc_rule(self):
        gc_rule = object()
        result = self._callFUT(gc_rule)
        self.assertTrue(result is gc_rule)

    def test_with_gc_rule_union(self):
        from gcloud_bigtable.column_family import GarbageCollectionRuleUnion

        gc_rule = GarbageCollectionRuleUnion()
        result = self._callFUT(gc_rule)
        self.assertTrue(result is gc_rule)

    def test_with_intersection_other_than_two(self):
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)

        gc_rule = GarbageCollectionRuleIntersection(rules=[])
        result = self._callFUT(gc_rule)
        self.assertTrue(result is gc_rule)

    def test_with_intersection_two_max_num_versions(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)

        rule1 = GarbageCollectionRule(max_num_versions=1)
        rule2 = GarbageCollectionRule(max_num_versions=2)
        gc_rule = GarbageCollectionRuleIntersection(rules=[rule1, rule2])
        result = self._callFUT(gc_rule)
        self.assertTrue(result is gc_rule)

    def test_with_intersection_two_rules(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)

        time_to_live = 101
        max_age = datetime.timedelta(seconds=time_to_live)
        rule1 = GarbageCollectionRule(max_age=max_age)
        max_versions = 2
        rule2 = GarbageCollectionRule(max_num_versions=max_versions)
        gc_rule = GarbageCollectionRuleIntersection(rules=[rule1, rule2])
        result = self._callFUT(gc_rule)
        expected_result = {
            'max_versions': max_versions,
            'time_to_live': time_to_live,
        }
        self.assertEqual(result, expected_result)

    def test_with_intersection_two_nested_rules(self):
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)

        rule1 = GarbageCollectionRuleIntersection(rules=[])
        rule2 = GarbageCollectionRuleIntersection(rules=[])
        gc_rule = GarbageCollectionRuleIntersection(rules=[rule1, rule2])
        result = self._callFUT(gc_rule)
        self.assertTrue(result is gc_rule)


class Test__convert_to_time_range(unittest2.TestCase):

    def _callFUT(self, timestamp=None):
        from gcloud_bigtable.happybase.table import _convert_to_time_range
        return _convert_to_time_range(timestamp=timestamp)

    def test_null(self):
        timestamp = None
        result = self._callFUT(timestamp=timestamp)
        self.assertEqual(result, None)

    def test_invalid_type(self):
        timestamp = object()
        with self.assertRaises(TypeError):
            self._callFUT(timestamp=timestamp)

    def test_success(self):
        from gcloud_bigtable._helpers import _microseconds_to_timestamp
        from gcloud_bigtable.row import TimestampRange

        timestamp = 1441928298571
        next_ts = _microseconds_to_timestamp(1000 * (timestamp + 1))
        result = self._callFUT(timestamp=timestamp)
        self.assertTrue(isinstance(result, TimestampRange))
        self.assertEqual(result.start, None)
        self.assertEqual(result.end, next_ts)


class Test__cells_to_pairs(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import _cells_to_pairs
        return _cells_to_pairs(*args, **kwargs)

    def test_without_timestamp(self):
        from gcloud_bigtable.row_data import Cell

        value1 = 'foo'
        cell1 = Cell(value=value1, timestamp=None)
        value2 = 'bar'
        cell2 = Cell(value=value2, timestamp=None)

        result = self._callFUT([cell1, cell2])
        self.assertEqual(result, [value1, value2])

    def test_with_timestamp(self):
        from gcloud_bigtable._helpers import _microseconds_to_timestamp
        from gcloud_bigtable.row_data import Cell

        value1 = 'foo'
        ts1_millis = 1221934570148
        ts1 = _microseconds_to_timestamp(ts1_millis * 1000)
        cell1 = Cell(value=value1, timestamp=ts1)

        value2 = 'bar'
        ts2_millis = 1221955575548
        ts2 = _microseconds_to_timestamp(ts2_millis * 1000)
        cell2 = Cell(value=value2, timestamp=ts2)

        result = self._callFUT([cell1, cell2], include_timestamp=True)
        self.assertEqual(result,
                         [(value1, ts1_millis), (value2, ts2_millis)])


class Test__filter_chain_helper(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import _filter_chain_helper
        return _filter_chain_helper(*args, **kwargs)

    def test_no_filters(self):
        with self.assertRaises(ValueError):
            self._callFUT()

    def test_single_filter(self):
        from gcloud_bigtable.row import RowFilter

        versions = 1337
        result = self._callFUT(versions=versions)
        self.assertTrue(isinstance(result, RowFilter))
        # Relies on the fact that RowFilter instances can
        # only have one value set.
        self.assertEqual(result.cells_per_column_limit_filter, versions)

    def test_existing_filters(self):
        from gcloud_bigtable.row import RowFilter

        filters = []
        versions = 1337
        result = self._callFUT(versions=versions, filters=filters)
        # Make sure filters has grown.
        self.assertEqual(filters, [result])

        self.assertTrue(isinstance(result, RowFilter))
        # Relies on the fact that RowFilter instances can
        # only have one value set.
        self.assertEqual(result.cells_per_column_limit_filter, versions)

    def _column_helper(self, num_filters, versions=None, timestamp=None):
        from gcloud_bigtable.row import RowFilter
        from gcloud_bigtable.row import RowFilterChain

        col_fam = 'cf1'
        qual = 'qual'
        column = col_fam + ':' + qual
        result = self._callFUT(column, versions=versions, timestamp=timestamp)
        self.assertTrue(isinstance(result, RowFilterChain))

        self.assertEqual(len(result.filters), num_filters)
        fam_filter = result.filters[0]
        qual_filter = result.filters[1]
        self.assertTrue(isinstance(fam_filter, RowFilter))
        self.assertTrue(isinstance(qual_filter, RowFilter))

        # Relies on the fact that RowFilter instances can
        # only have one value set.
        self.assertEqual(fam_filter.family_name_regex_filter, col_fam)
        self.assertEqual(qual_filter.column_qualifier_regex_filter, qual)

        return result

    def test_column_only(self):
        self._column_helper(num_filters=2)

    def test_with_versions(self):
        from gcloud_bigtable.row import RowFilter

        versions = 11
        result = self._column_helper(num_filters=3, versions=versions)

        version_filter = result.filters[2]
        self.assertTrue(isinstance(version_filter, RowFilter))
        # Relies on the fact that RowFilter instances can
        # only have one value set.
        self.assertEqual(version_filter.cells_per_column_limit_filter,
                         versions)

    def test_with_timestamp(self):
        from gcloud_bigtable._helpers import _microseconds_to_timestamp
        from gcloud_bigtable.row import RowFilter
        from gcloud_bigtable.row import TimestampRange

        timestamp = 1441928298571
        result = self._column_helper(num_filters=3, timestamp=timestamp)

        range_filter = result.filters[2]
        self.assertTrue(isinstance(range_filter, RowFilter))
        # Relies on the fact that RowFilter instances can
        # only have one value set.
        time_range = range_filter.timestamp_range_filter
        self.assertTrue(isinstance(time_range, TimestampRange))
        self.assertEqual(time_range.start, None)
        next_ts = _microseconds_to_timestamp(1000 * (timestamp + 1))
        self.assertEqual(time_range.end, next_ts)

    def test_with_all_options(self):
        versions = 11
        timestamp = 1441928298571
        self._column_helper(num_filters=4, versions=versions,
                            timestamp=timestamp)


class Test__columns_filter_helper(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import _columns_filter_helper
        return _columns_filter_helper(*args, **kwargs)

    def test_no_columns(self):
        columns = []
        with self.assertRaises(ValueError):
            self._callFUT(columns)

    def test_single_column(self):
        from gcloud_bigtable.row import RowFilter

        col_fam = 'cf1'
        columns = [col_fam]
        result = self._callFUT(columns)
        expected_result = RowFilter(family_name_regex_filter=col_fam)
        self.assertEqual(result, expected_result)

    def test_column_and_column_familieis(self):
        from gcloud_bigtable.row import RowFilter
        from gcloud_bigtable.row import RowFilterChain
        from gcloud_bigtable.row import RowFilterUnion

        col_fam1 = 'cf1'
        col_fam2 = 'cf2'
        col_qual2 = 'qual2'
        columns = [col_fam1, col_fam2 + ':' + col_qual2]
        result = self._callFUT(columns)

        self.assertTrue(isinstance(result, RowFilterUnion))
        self.assertEqual(len(result.filters), 2)
        filter1 = result.filters[0]
        filter2 = result.filters[1]

        self.assertTrue(isinstance(filter1, RowFilter))
        self.assertEqual(filter1.family_name_regex_filter, col_fam1)

        self.assertTrue(isinstance(filter2, RowFilterChain))
        filter2a, filter2b = filter2.filters
        self.assertTrue(isinstance(filter2a, RowFilter))
        self.assertEqual(filter2a.family_name_regex_filter, col_fam2)
        self.assertTrue(isinstance(filter2b, RowFilter))
        self.assertEqual(filter2b.column_qualifier_regex_filter, col_qual2)


class Test__row_keys_filter_helper(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.table import _row_keys_filter_helper
        return _row_keys_filter_helper(*args, **kwargs)

    def test_no_rows(self):
        row_keys = []
        with self.assertRaises(ValueError):
            self._callFUT(row_keys)

    def test_single_row(self):
        from gcloud_bigtable.row import RowFilter

        row_key = b'row-key'
        row_keys = [row_key]
        result = self._callFUT(row_keys)
        expected_result = RowFilter(row_key_regex_filter=row_key)
        self.assertEqual(result, expected_result)

    def test_many_rows(self):
        from gcloud_bigtable.row import RowFilter
        from gcloud_bigtable.row import RowFilterUnion

        row_key1 = b'row-key1'
        row_key2 = b'row-key2'
        row_key3 = b'row-key3'
        row_keys = [row_key1, row_key2, row_key3]
        result = self._callFUT(row_keys)

        filter1 = RowFilter(row_key_regex_filter=row_key1)
        filter2 = RowFilter(row_key_regex_filter=row_key2)
        filter3 = RowFilter(row_key_regex_filter=row_key3)
        expected_result = RowFilterUnion(filters=[filter1, filter2, filter3])
        self.assertEqual(result, expected_result)


class TestTable(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.happybase.table import Table
        return Table

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        cluster = object()
        connection = _Connection(cluster)
        tables_constructed = []

        def make_low_level_table(*args, **kwargs):
            result = _MockLowLevelTable(*args, **kwargs)
            tables_constructed.append(result)
            return result

        with _Monkey(MUT, _LowLevelTable=make_low_level_table):
            table = self._makeOne(name, connection)
        self.assertEqual(table.name, name)
        self.assertEqual(table.connection, connection)

        table_instance, = tables_constructed
        self.assertEqual(table._low_level_table, table_instance)
        self.assertEqual(table_instance.args, (name, cluster))
        self.assertEqual(table_instance.kwargs, {})

    def test_constructor_null_connection(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        self.assertEqual(table.name, name)
        self.assertEqual(table.connection, connection)
        self.assertEqual(table._low_level_table, None)

    def test___repr__(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        self.assertEqual(repr(table), '<table.Table name=\'table-name\'>')

    def test_families(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()

        # Mock the column families to be returned.
        col_fam_name = 'fam'
        gc_rule = object()
        col_fam = _MockLowLevelColumnFamily(col_fam_name, gc_rule=gc_rule)
        col_fams = {col_fam_name: col_fam}
        table._low_level_table.column_families = col_fams

        to_dict_result = object()
        mock_gc_rule_to_dict = _MockCalled(to_dict_result)
        with _Monkey(MUT, _gc_rule_to_dict=mock_gc_rule_to_dict):
            result = table.families()

        self.assertEqual(result, {col_fam_name: to_dict_result})
        self.assertEqual(table._low_level_table.list_column_families_calls, 1)

        # Check the input to our mock.
        mock_gc_rule_to_dict.check_called(self, [(gc_rule,)])

    def test_regions(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)

        with self.assertRaises(NotImplementedError):
            table.regions()

    def test_row_empty_row(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()
        table._low_level_table.read_row_result = None

        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)

        row_key = 'row-key'
        timestamp = object()
        with _Monkey(MUT, _filter_chain_helper=mock_filter_chain_helper):
            result = table.row(row_key, timestamp=timestamp)

        # read_row_result == None --> No results.
        self.assertEqual(result, {})

        read_row_args = (row_key,)
        read_row_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_row_calls, [
            (read_row_args, read_row_kwargs),
        ])

        expected_kwargs = {
            'filters': [],
            'versions': 1,
            'timestamp': timestamp,
        }
        mock_filter_chain_helper.check_called(self, [()], [expected_kwargs])

    def test_row_with_columns(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()
        table._low_level_table.read_row_result = None

        fake_col_filter = object()
        mock_columns_filter_helper = _MockCalled(fake_col_filter)
        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)

        row_key = 'row-key'
        columns = object()
        with _Monkey(MUT, _filter_chain_helper=mock_filter_chain_helper,
                     _columns_filter_helper=mock_columns_filter_helper):
            result = table.row(row_key, columns=columns)

        # read_row_result == None --> No results.
        self.assertEqual(result, {})

        read_row_args = (row_key,)
        read_row_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_row_calls, [
            (read_row_args, read_row_kwargs),
        ])

        mock_columns_filter_helper.check_called(self, [(columns,)])
        expected_kwargs = {
            'filters': [fake_col_filter],
            'versions': 1,
            'timestamp': None,
        }
        mock_filter_chain_helper.check_called(self, [()], [expected_kwargs])

    def test_row_with_results(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT
        from gcloud_bigtable.row_data import PartialRowData

        row_key = 'row-key'
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()
        partial_row = PartialRowData(row_key)
        table._low_level_table.read_row_result = partial_row

        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)
        fake_pair = object()
        mock_cells_to_pairs = _MockCalled([fake_pair])

        col_fam = u'cf1'
        qual = b'qual'
        fake_cells = object()
        partial_row._cells = {col_fam: {qual: fake_cells}}
        include_timestamp = object()
        with _Monkey(MUT, _filter_chain_helper=mock_filter_chain_helper,
                     _cells_to_pairs=mock_cells_to_pairs):
            result = table.row(row_key, include_timestamp=include_timestamp)

        # The results come from _cells_to_pairs.
        expected_result = {col_fam.encode('ascii') + b':' + qual: fake_pair}
        self.assertEqual(result, expected_result)

        read_row_args = (row_key,)
        read_row_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_row_calls, [
            (read_row_args, read_row_kwargs),
        ])

        expected_kwargs = {
            'filters': [],
            'versions': 1,
            'timestamp': None,
        }
        mock_filter_chain_helper.check_called(self, [()], [expected_kwargs])
        to_pairs_kwargs = {'include_timestamp': include_timestamp}
        mock_cells_to_pairs.check_called(
            self, [(fake_cells,)], [to_pairs_kwargs])

    def test_rows_empty_row(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)

        result = table.rows([])
        self.assertEqual(result, [])

    def test_rows_with_columns(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()
        rr_result = _MockPartialRowsData()
        table._low_level_table.read_rows_result = rr_result
        self.assertEqual(rr_result.consume_all_calls, 0)

        fake_col_filter = object()
        mock_columns_filter_helper = _MockCalled(fake_col_filter)
        fake_rows_filter = object()
        mock_row_keys_filter_helper = _MockCalled(fake_rows_filter)
        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)

        rows = ['row-key']
        columns = object()
        with _Monkey(MUT, _filter_chain_helper=mock_filter_chain_helper,
                     _row_keys_filter_helper=mock_row_keys_filter_helper,
                     _columns_filter_helper=mock_columns_filter_helper):
            result = table.rows(rows, columns=columns)

        # read_rows_result == Empty PartialRowsData --> No results.
        self.assertEqual(result, [])

        read_rows_args = ()
        read_rows_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_rows_calls, [
            (read_rows_args, read_rows_kwargs),
        ])
        self.assertEqual(rr_result.consume_all_calls, 1)

        mock_columns_filter_helper.check_called(self, [(columns,)])
        mock_row_keys_filter_helper.check_called(self, [(rows,)])
        expected_kwargs = {
            'filters': [fake_col_filter, fake_rows_filter],
            'versions': 1,
            'timestamp': None,
        }
        mock_filter_chain_helper.check_called(self, [()], [expected_kwargs])

    def test_rows_with_results(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT
        from gcloud_bigtable.row_data import PartialRowData

        row_key1 = 'row-key1'
        row_key2 = 'row-key2'
        rows = [row_key1, row_key2]
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()

        row1 = PartialRowData(row_key1)
        # Return row1 but not row2
        rr_result = _MockPartialRowsData(rows={row_key1: row1})
        table._low_level_table.read_rows_result = rr_result
        self.assertEqual(rr_result.consume_all_calls, 0)

        fake_rows_filter = object()
        mock_row_keys_filter_helper = _MockCalled(fake_rows_filter)
        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)
        fake_pair = object()
        mock_cells_to_pairs = _MockCalled([fake_pair])

        col_fam = u'cf1'
        qual = b'qual'
        fake_cells = object()
        row1._cells = {col_fam: {qual: fake_cells}}
        include_timestamp = object()
        with _Monkey(MUT, _row_keys_filter_helper=mock_row_keys_filter_helper,
                     _filter_chain_helper=mock_filter_chain_helper,
                     _cells_to_pairs=mock_cells_to_pairs):
            result = table.rows(rows, include_timestamp=include_timestamp)

        # read_rows_result == PartialRowsData with row_key1
        expected_result = {col_fam.encode('ascii') + b':' + qual: fake_pair}
        self.assertEqual(result, [(row_key1, expected_result)])

        read_rows_args = ()
        read_rows_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_rows_calls, [
            (read_rows_args, read_rows_kwargs),
        ])
        self.assertEqual(rr_result.consume_all_calls, 1)

        mock_row_keys_filter_helper.check_called(self, [(rows,)])
        expected_kwargs = {
            'filters': [fake_rows_filter],
            'versions': 1,
            'timestamp': None,
        }
        mock_filter_chain_helper.check_called(self, [()], [expected_kwargs])
        to_pairs_kwargs = {'include_timestamp': include_timestamp}
        mock_cells_to_pairs.check_called(
            self, [(fake_cells,)], [to_pairs_kwargs])

    def test_cells_empty_row(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()
        table._low_level_table.read_row_result = None

        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)

        row_key = 'row-key'
        column = 'fam:col1'
        with _Monkey(MUT, _filter_chain_helper=mock_filter_chain_helper):
            result = table.cells(row_key, column)

        # read_row_result == None --> No results.
        self.assertEqual(result, [])

        read_row_args = (row_key,)
        read_row_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_row_calls, [
            (read_row_args, read_row_kwargs),
        ])

        expected_kwargs = {
            'column': column,
            'versions': None,
            'timestamp': None,
        }
        mock_filter_chain_helper.check_called(self, [()], [expected_kwargs])

    def test_cells_with_results(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT
        from gcloud_bigtable.row_data import PartialRowData

        row_key = 'row-key'
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        table._low_level_table = _MockLowLevelTable()
        partial_row = PartialRowData(row_key)
        table._low_level_table.read_row_result = partial_row

        # These are all passed to mocks.
        versions = object()
        timestamp = object()
        include_timestamp = object()

        fake_filter = object()
        mock_filter_chain_helper = _MockCalled(fake_filter)
        fake_result = object()
        mock_cells_to_pairs = _MockCalled(fake_result)

        col_fam = 'cf1'
        qual = 'qual'
        fake_cells = object()
        partial_row._cells = {col_fam: {qual: fake_cells}}
        column = col_fam + ':' + qual
        with _Monkey(MUT, _filter_chain_helper=mock_filter_chain_helper,
                     _cells_to_pairs=mock_cells_to_pairs):
            result = table.cells(row_key, column, versions=versions,
                                 timestamp=timestamp,
                                 include_timestamp=include_timestamp)

        self.assertEqual(result, fake_result)

        read_row_args = (row_key,)
        read_row_kwargs = {'filter_': fake_filter}
        self.assertEqual(table._low_level_table.read_row_calls, [
            (read_row_args, read_row_kwargs),
        ])

        filter_kwargs = {
            'column': column,
            'versions': versions,
            'timestamp': timestamp,
        }
        mock_filter_chain_helper.check_called(self, [()], [filter_kwargs])
        to_pairs_kwargs = {'include_timestamp': include_timestamp}
        mock_cells_to_pairs.check_called(
            self, [(fake_cells,)], [to_pairs_kwargs])

    def test_scan_with_batch_size(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        with self.assertRaises(ValueError):
            table.scan(batch_size=object())

    def test_scan_with_scan_batching(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        with self.assertRaises(ValueError):
            table.scan(scan_batching=object())

    def test_scan_with_invalid_limit(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        with self.assertRaises(ValueError):
            table.scan(limit=-10)

    def test_scan_with_row_prefix_and_row_start(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        with self.assertRaises(ValueError):
            table.scan(row_prefix='a', row_stop='abc')

    def test_scan(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)

        row_start = 'row-start'
        row_stop = 'row-stop'
        columns = ['fam:col1', 'fam:col2']
        filter_ = 'KeyOnlyFilter ()'
        timestamp = None
        include_timestamp = True
        limit = 123
        sorted_columns = True
        with self.assertRaises(NotImplementedError):
            table.scan(row_start=row_start, row_stop=row_stop,
                       columns=columns, filter=filter_,
                       timestamp=timestamp,
                       include_timestamp=include_timestamp,
                       limit=limit, sorted_columns=sorted_columns)

    def test_put(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT
        from gcloud_bigtable.happybase.table import _WAL_SENTINEL

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        batches_created = []

        def make_batch(*args, **kwargs):
            result = _MockBatch(*args, **kwargs)
            batches_created.append(result)
            return result

        row = 'row-key'
        data = {'fam:col': 'foo'}
        timestamp = None
        with _Monkey(MUT, Batch=make_batch):
            result = table.put(row, data, timestamp=timestamp)

        # There is no return value.
        self.assertEqual(result, None)

        # Check how the batch was created and used.
        batch, = batches_created
        self.assertTrue(isinstance(batch, _MockBatch))
        self.assertEqual(batch.args, (table,))
        expected_kwargs = {
            'timestamp': timestamp,
            'batch_size': None,
            'transaction': False,
            'wal': _WAL_SENTINEL,
        }
        self.assertEqual(batch.kwargs, expected_kwargs)
        # Make sure it was a successful context manager
        self.assertEqual(batch.exit_vals, [(None, None, None)])
        self.assertEqual(batch.put_args, [(row, data)])
        self.assertEqual(batch.delete_args, [])

    def test_delete(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT
        from gcloud_bigtable.happybase.table import _WAL_SENTINEL

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        batches_created = []

        def make_batch(*args, **kwargs):
            result = _MockBatch(*args, **kwargs)
            batches_created.append(result)
            return result

        row = 'row-key'
        columns = ['fam:col1', 'fam:col2']
        timestamp = None
        with _Monkey(MUT, Batch=make_batch):
            result = table.delete(row, columns=columns, timestamp=timestamp)

        # There is no return value.
        self.assertEqual(result, None)

        # Check how the batch was created and used.
        batch, = batches_created
        self.assertTrue(isinstance(batch, _MockBatch))
        self.assertEqual(batch.args, (table,))
        expected_kwargs = {
            'timestamp': timestamp,
            'batch_size': None,
            'transaction': False,
            'wal': _WAL_SENTINEL,
        }
        self.assertEqual(batch.kwargs, expected_kwargs)
        # Make sure it was a successful context manager
        self.assertEqual(batch.exit_vals, [(None, None, None)])
        self.assertEqual(batch.put_args, [])
        self.assertEqual(batch.delete_args, [(row, columns)])

    def test_batch(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)

        timestamp = object()
        batch_size = 42
        transaction = False  # Must be False when batch_size is non-null
        wal = object()

        with _Monkey(MUT, Batch=_MockBatch):
            result = table.batch(timestamp=timestamp, batch_size=batch_size,
                                 transaction=transaction, wal=wal)

        self.assertTrue(isinstance(result, _MockBatch))
        self.assertEqual(result.args, (table,))
        expected_kwargs = {
            'timestamp': timestamp,
            'batch_size': batch_size,
            'transaction': transaction,
            'wal': wal,
        }
        self.assertEqual(result.kwargs, expected_kwargs)

    def test_counter_get(self):
        klass = self._getTargetClass()
        counter_value = 1337

        class TableWithInc(klass):

            incremented = []
            value = counter_value

            def counter_inc(self, row, column, value=1):
                self.incremented.append((row, column, value))
                self.value += value
                return self.value

        name = 'table-name'
        connection = None
        table = TableWithInc(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        self.assertEqual(TableWithInc.incremented, [])
        result = table.counter_get(row, column)
        self.assertEqual(result, counter_value)
        self.assertEqual(TableWithInc.incremented, [(row, column, 0)])

    def test_counter_set(self):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        value = 42
        with self.assertRaises(NotImplementedError):
            table.counter_set(row, column, value=value)

    def test_counter_dec(self):
        klass = self._getTargetClass()
        counter_value = 42

        class TableWithInc(klass):

            incremented = []
            value = counter_value

            def counter_inc(self, row, column, value=1):
                self.incremented.append((row, column, value))
                self.value += value
                return self.value

        name = 'table-name'
        connection = None
        table = TableWithInc(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        dec_value = 987
        self.assertEqual(TableWithInc.incremented, [])
        result = table.counter_dec(row, column, value=dec_value)
        self.assertEqual(result, counter_value - dec_value)
        self.assertEqual(TableWithInc.incremented, [(row, column, -dec_value)])

    def _counter_inc_helper(self, row, column, value, commit_result):
        name = 'table-name'
        connection = None
        table = self._makeOne(name, connection)
        # Mock the return values.
        table._low_level_table = _MockLowLevelTable()
        table._low_level_table.row_values[row] = _MockLowLevelRow(
            row, commit_result=commit_result)

        result = table.counter_inc(row, column, value=value)

        incremented_value = value + _MockLowLevelRow.COUNTER_DEFAULT
        self.assertEqual(result, incremented_value)

        # Check the row values returned.
        row_obj = table._low_level_table.row_values[row]
        self.assertEqual(row_obj.counts,
                         {tuple(column.split(':')): incremented_value})

    def test_counter_inc(self):
        import struct

        row = 'row-key'
        col_fam = 'fam'
        col_qual = 'col1'
        column = col_fam + ':' + col_qual
        value = 42
        packed_value = struct.pack('>q', value)
        fake_timestamp = None
        commit_result = {
            col_fam: {
                col_qual: [(packed_value, fake_timestamp)],
            }
        }
        self._counter_inc_helper(row, column, value, commit_result)

    def test_counter_inc_bad_result(self):
        row = 'row-key'
        col_fam = 'fam'
        col_qual = 'col1'
        column = col_fam + ':' + col_qual
        value = 42
        commit_result = None
        with self.assertRaises(TypeError):
            self._counter_inc_helper(row, column, value, commit_result)

    def test_counter_inc_result_key_error(self):
        row = 'row-key'
        col_fam = 'fam'
        col_qual = 'col1'
        column = col_fam + ':' + col_qual
        value = 42
        commit_result = {}
        with self.assertRaises(KeyError):
            self._counter_inc_helper(row, column, value, commit_result)

    def test_counter_inc_result_nested_key_error(self):
        row = 'row-key'
        col_fam = 'fam'
        col_qual = 'col1'
        column = col_fam + ':' + col_qual
        value = 42
        commit_result = {col_fam: {}}
        with self.assertRaises(KeyError):
            self._counter_inc_helper(row, column, value, commit_result)

    def test_counter_inc_result_non_unique_cell(self):
        row = 'row-key'
        col_fam = 'fam'
        col_qual = 'col1'
        column = col_fam + ':' + col_qual
        value = 42
        fake_timestamp = None
        packed_value = None
        commit_result = {
            col_fam: {
                col_qual: [
                    (packed_value, fake_timestamp),
                    (packed_value, fake_timestamp),
                ],
            }
        }
        with self.assertRaises(ValueError):
            self._counter_inc_helper(row, column, value, commit_result)


class _MockLowLevelTable(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.list_column_families_calls = 0
        self.column_families = {}
        self.row_values = {}
        self.read_row_calls = []
        self.read_row_result = None
        self.read_rows_calls = []
        self.read_rows_result = None

    def list_column_families(self):
        self.list_column_families_calls += 1
        return self.column_families

    def row(self, row_key):
        return self.row_values[row_key]

    def read_row(self, *args, **kwargs):
        self.read_row_calls.append((args, kwargs))
        return self.read_row_result

    def read_rows(self, *args, **kwargs):
        self.read_rows_calls.append((args, kwargs))
        return self.read_rows_result


class _MockLowLevelColumnFamily(object):

    def __init__(self, column_family_id, gc_rule=None):
        self.column_family_id = column_family_id
        self.gc_rule = gc_rule


class _Connection(object):

    def __init__(self, cluster):
        self._cluster = cluster


class _MockBatch(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.exit_vals = []
        self.put_args = []
        self.delete_args = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.exit_vals.append((exc_type, exc_value, traceback))

    def put(self, *args):
        self.put_args.append(args)

    def delete(self, *args):
        self.delete_args.append(args)


class _MockLowLevelRow(object):

    COUNTER_DEFAULT = 0

    def __init__(self, row_key, commit_result=None):
        self.row_key = row_key
        self.counts = {}
        self.commit_result = commit_result

    def increment_cell_value(self, column_family_id, column, int_value):
        count = self.counts.setdefault((column_family_id, column),
                                       self.COUNTER_DEFAULT)
        self.counts[(column_family_id, column)] = count + int_value

    def commit_modifications(self):
        return self.commit_result


class _MockPartialRowsData(object):

    def __init__(self, rows=None):
        self.rows = rows or {}
        self.consume_all_calls = 0

    def consume_all(self):
        self.consume_all_calls += 1
