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
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row = 'row-key'
        data = {'fam:col': 'foo'}
        timestamp = None
        with self.assertRaises(NotImplementedError):
            table.put(row, data, timestamp=timestamp)

    def test_delete(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row = 'row-key'
        columns = ['fam:col1', 'fam:col2']
        timestamp = None
        with self.assertRaises(NotImplementedError):
            table.delete(row, columns=columns, timestamp=timestamp)

    def test_batch(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import table as MUT

        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        timestamp = object()
        batch_size = 42
        transaction = False  # Must be False when batch_size is non-null
        wal = object()

        batch_result = object()
        mock_batch = _MockCalled(batch_result)
        with _Monkey(MUT, Batch=mock_batch):
            result = table.batch(timestamp=timestamp, batch_size=batch_size,
                                 transaction=transaction, wal=wal)

        self.assertEqual(result, batch_result)
        expected_kwargs = {
            'timestamp': timestamp,
            'batch_size': batch_size,
            'transaction': transaction,
            'wal': wal,
        }
        mock_batch.check_called(self, [(table,)], [expected_kwargs])

    def test_counter_get(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        with self.assertRaises(NotImplementedError):
            table.counter_get(row, column)

    def test_counter_set(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        value = 42
        with self.assertRaises(NotImplementedError):
            table.counter_set(row, column, value=value)

    def test_counter_inc(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        value = 42
        with self.assertRaises(NotImplementedError):
            table.counter_inc(row, column, value=value)

    def test_counter_dec(self):
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        row = 'row-key'
        column = 'fam:col1'
        value = 42
        with self.assertRaises(NotImplementedError):
            table.counter_dec(row, column, value=value)


class _MockLowLevelTable(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.list_column_families_calls = 0
        self.column_families = {}

    def list_column_families(self):
        self.list_column_families_calls += 1
        return self.column_families


class _MockLowLevelColumnFamily(object):

    def __init__(self, column_family_id, gc_rule=None):
        self.column_family_id = column_family_id
        self.gc_rule = gc_rule


class _Connection(object):

    def __init__(self, cluster):
        self._cluster = cluster
