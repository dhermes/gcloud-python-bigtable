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

    def _callFUT(self, gc_rule):
        from gcloud_bigtable.happybase.table import _gc_rule_to_dict
        return _gc_rule_to_dict(gc_rule)

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


class TestTable(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.happybase.table import Table
        return Table

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)
        self.assertEqual(table.name, name)
        self.assertEqual(table.connection, connection)

    def test___repr__(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)
        self.assertEqual(repr(table), '<table.Table name=\'table-name\'>')

    def test_families(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        cluster = object()
        connection = _Connection(cluster)
        table = self._makeOne(name, connection)

        tables_constructed = []
        col_fam_name = 'fam'
        gc_rule = object()
        col_fam = _MockLowLevelColumnFamily(col_fam_name, gc_rule=gc_rule)
        col_fams = {col_fam_name: col_fam}

        def make_low_level_table(*args, **kwargs):
            result = _MockLowLevelTable(*args, **kwargs)
            result.column_families = col_fams
            tables_constructed.append(result)
            return result

        to_dict_result = object()
        mock_gc_rule_to_dict = _MockCalled(to_dict_result)
        with _Monkey(MUT, _LowLevelTable=make_low_level_table,
                     _gc_rule_to_dict=mock_gc_rule_to_dict):
            result = table.families()

        self.assertEqual(result, {col_fam_name: to_dict_result})

        # Just one table would have been created.
        table_instance, = tables_constructed
        self.assertEqual(table_instance.args, (name, cluster))
        self.assertEqual(table_instance.kwargs, {})
        self.assertEqual(table_instance.list_column_families_calls, 1)

        # Check the input to our mock.
        mock_gc_rule_to_dict.check_called(self, [(gc_rule,)])

    def test_regions(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        with self.assertRaises(NotImplementedError):
            table.regions()

    def test_row(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row_key = 'row-key'
        columns = ['fam:col1', 'fam:col2']
        timestamp = None
        include_timestamp = True
        with self.assertRaises(NotImplementedError):
            table.row(row_key, columns=columns, timestamp=timestamp,
                      include_timestamp=include_timestamp)

    def test_rows(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row_keys = ['row-key']
        columns = ['fam:col1', 'fam:col2']
        timestamp = None
        include_timestamp = True
        with self.assertRaises(NotImplementedError):
            table.rows(row_keys, columns=columns, timestamp=timestamp,
                       include_timestamp=include_timestamp)

    def test_cells(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row_key = 'row-key'
        column = 'fam:col1'
        versions = 11
        timestamp = None
        include_timestamp = True
        with self.assertRaises(NotImplementedError):
            table.cells(row_key, column, versions=versions,
                        timestamp=timestamp,
                        include_timestamp=include_timestamp)

    def test_scan(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row_start = 'row-start'
        row_stop = 'row-stop'
        row_prefix = 'row-prefix'
        columns = ['fam:col1', 'fam:col2']
        filter_ = 'KeyOnlyFilter ()'
        timestamp = None
        include_timestamp = True
        batch_size = 1337
        scan_batching = None
        limit = 123
        sorted_columns = True
        with self.assertRaises(NotImplementedError):
            table.scan(row_start=row_start, row_stop=row_stop,
                       row_prefix=row_prefix, columns=columns, filter=filter_,
                       timestamp=timestamp,
                       include_timestamp=include_timestamp,
                       batch_size=batch_size, scan_batching=scan_batching,
                       limit=limit, sorted_columns=sorted_columns)

    def test_put(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT
        from gcloud_bigtable.happybase.table import _WAL_SENTINEL

        name = 'table-name'
        connection = object()
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
        connection = object()
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
        connection = object()
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
        connection = object()
        table = TableWithInc(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        self.assertEqual(TableWithInc.incremented, [])
        result = table.counter_get(row, column)
        self.assertEqual(result, counter_value)
        self.assertEqual(TableWithInc.incremented, [(row, column, 0)])

    def test_counter_set(self):
        name = 'table-name'
        connection = object()
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
        connection = object()
        table = TableWithInc(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        dec_value = 987
        self.assertEqual(TableWithInc.incremented, [])
        result = table.counter_dec(row, column, value=dec_value)
        self.assertEqual(result, counter_value - dec_value)
        self.assertEqual(TableWithInc.incremented, [(row, column, -dec_value)])

    def _counter_inc_helper(self, row, column, value, commit_result):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        cluster = object()
        connection = _Connection(cluster)
        table = self._makeOne(name, connection)
        tables_constructed = []

        def make_low_level_table(*args, **kwargs):
            result = _MockLowLevelTable(*args, **kwargs)
            tables_constructed.append(result)
            result.row_values[row] = _MockLowLevelRow(
                row, commit_result=commit_result)
            return result

        with _Monkey(MUT, _LowLevelTable=make_low_level_table):
            result = table.counter_inc(row, column, value=value)

        incremented_value = value + _MockLowLevelRow.COUNTER_DEFAULT
        self.assertEqual(result, incremented_value)

        # Just one table would have been created.
        table_instance, = tables_constructed
        self.assertEqual(table_instance.args, (name, cluster))
        self.assertEqual(table_instance.kwargs, {})

        # Check the row values returned.
        row_obj = table_instance.row_values[row]
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

    def list_column_families(self):
        self.list_column_families_calls += 1
        return self.column_families

    def row(self, row_key):
        return self.row_values[row_key]


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
