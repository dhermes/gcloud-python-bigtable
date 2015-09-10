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


class _SendMixin(object):

    _send_called = False

    def send(self):
        self._send_called = True


class Test__get_column_pairs(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.happybase.batch import _get_column_pairs
        return _get_column_pairs(*args, **kwargs)

    def test_it(self):
        columns = ['cf1', 'cf2:', 'cf3::', 'cf3:name1', 'cf3:name2']
        result = self._callFUT(columns)
        expected_result = [
            ['cf1', None],
            ['cf2', None],
            ['cf3', None],
            ['cf3', 'name1'],
            ['cf3', 'name2'],
        ]
        self.assertEqual(result, expected_result)

    def test_bad_column(self):
        columns = ['a:b:c']
        with self.assertRaises(ValueError):
            self._callFUT(columns)

    def test_bad_column_type(self):
        columns = [None]
        with self.assertRaises(AttributeError):
            self._callFUT(columns)

    def test_bad_columns_var(self):
        columns = None
        with self.assertRaises(TypeError):
            self._callFUT(columns)

    def test_column_family_with_require_qualifier(self):
        columns = ['a:']
        with self.assertRaises(ValueError):
            self._callFUT(columns, require_qualifier=True)


class TestBatch(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.happybase.batch import Batch
        return Batch

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        table = object()
        batch = self._makeOne(table)
        self.assertEqual(batch._table, table)
        self.assertEqual(batch._batch_size, None)
        self.assertEqual(batch._timestamp, None)
        self.assertEqual(batch._delete_range, None)
        self.assertEqual(batch._transaction, False)

    def test_constructor_explicit(self):
        from gcloud_bigtable._helpers import _microseconds_to_timestamp
        from gcloud_bigtable.row import TimestampRange

        table = object()
        timestamp = 144185290431
        batch_size = 42
        transaction = False  # Must be False when batch_size is non-null

        batch = self._makeOne(table, timestamp=timestamp,
                              batch_size=batch_size, transaction=transaction)
        self.assertEqual(batch._table, table)
        self.assertEqual(batch._batch_size, batch_size)
        self.assertEqual(batch._timestamp,
                         _microseconds_to_timestamp(1000 * timestamp))

        next_timestamp = _microseconds_to_timestamp(1000 * (timestamp + 1))
        time_range = TimestampRange(end=next_timestamp)
        self.assertEqual(batch._delete_range, time_range)
        self.assertEqual(batch._transaction, transaction)

    def test_constructor_with_non_default_wal(self):
        table = object()
        wal = object()
        with self.assertRaises(ValueError):
            self._makeOne(table, wal=wal)

    def test_constructor_with_non_positive_batch_size(self):
        table = object()
        batch_size = -10
        with self.assertRaises(ValueError):
            self._makeOne(table, batch_size=batch_size)
        batch_size = 0
        with self.assertRaises(ValueError):
            self._makeOne(table, batch_size=batch_size)

    def test_constructor_with_batch_size_and_transactional(self):
        table = object()
        batch_size = 1
        transaction = True
        with self.assertRaises(TypeError):
            self._makeOne(table, batch_size=batch_size,
                          transaction=transaction)

    def test_send(self):
        table = object()
        batch = self._makeOne(table)

        batch._row_map = row_map = _MockRowMap()
        row_map['row-key1'] = row1 = _MockRow()
        row_map['row-key2'] = row2 = _MockRow()
        batch._mutation_count = 1337

        self.assertEqual(row_map.clear_count, 0)
        self.assertEqual(row1.commits, 0)
        self.assertEqual(row2.commits, 0)
        self.assertNotEqual(batch._mutation_count, 0)
        self.assertNotEqual(row_map, {})

        batch.send()
        self.assertEqual(row_map.clear_count, 1)
        self.assertEqual(row1.commits, 1)
        self.assertEqual(row2.commits, 1)
        self.assertEqual(batch._mutation_count, 0)
        self.assertEqual(row_map, {})

    def test__try_send_no_batch_size(self):
        klass = self._getTargetClass()

        class BatchWithSend(_SendMixin, klass):
            pass

        table = object()
        batch = BatchWithSend(table)

        self.assertEqual(batch._batch_size, None)
        self.assertFalse(batch._send_called)
        batch._try_send()
        self.assertFalse(batch._send_called)

    def test__try_send_too_few_mutations(self):
        klass = self._getTargetClass()

        class BatchWithSend(_SendMixin, klass):
            pass

        table = object()
        batch_size = 10
        batch = BatchWithSend(table, batch_size=batch_size)

        self.assertEqual(batch._batch_size, batch_size)
        self.assertFalse(batch._send_called)
        mutation_count = 2
        batch._mutation_count = mutation_count
        self.assertTrue(mutation_count < batch_size)
        batch._try_send()
        self.assertFalse(batch._send_called)

    def test__try_send_actual_send(self):
        klass = self._getTargetClass()

        class BatchWithSend(_SendMixin, klass):
            pass

        table = object()
        batch_size = 10
        batch = BatchWithSend(table, batch_size=batch_size)

        self.assertEqual(batch._batch_size, batch_size)
        self.assertFalse(batch._send_called)
        mutation_count = 12
        batch._mutation_count = mutation_count
        self.assertTrue(mutation_count > batch_size)
        batch._try_send()
        self.assertTrue(batch._send_called)

    def test__get_row_exists(self):
        table = object()
        batch = self._makeOne(table)

        row_key = 'row-key'
        row_obj = object()
        batch._row_map[row_key] = row_obj
        result = batch._get_row(row_key)
        self.assertEqual(result, row_obj)

    def test__get_row_create_new(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import batch as MUT

        # Make mock batch and make sure we can create a low-level table.
        cluster = object()
        connection = _MockConnection(cluster)
        name = 'name'
        table = _MockTable(name, connection)
        batch = self._makeOne(table)

        # Make sure row map is empty.
        self.assertEqual(batch._row_map, {})

        # Customize/capture mock table creation.
        row_key = 'row-key'
        mock_row = object()
        tables_constructed = []

        def make_low_level_table(*args, **kwargs):
            result = _MockLowLevelTable(*args, **kwargs)
            tables_constructed.append(result)
            result.mock_row = mock_row
            return result

        # Actually get the row (which creates a row via a low-level table).
        with _Monkey(MUT, _LowLevelTable=make_low_level_table):
            result = batch._get_row(row_key)

        self.assertEqual(result, mock_row)

        # Check all the things that were constructed.
        table_instance, = tables_constructed
        self.assertEqual(table_instance.args, (name, cluster))
        self.assertEqual(table_instance.kwargs, {})
        self.assertEqual(table_instance.rows_made, [row_key])
        # Check how the batch was updated.
        self.assertEqual(batch._row_map, {row_key: mock_row})

    def test_put_bad_wal(self):
        from gcloud_bigtable.happybase.batch import _WAL_SENTINEL

        table = object()
        batch = self._makeOne(table)

        row = 'row-key'
        data = {}
        wal = None

        self.assertNotEqual(wal, _WAL_SENTINEL)
        with self.assertRaises(ValueError):
            batch.put(row, data, wal=wal)

    def test_put(self):
        import operator

        table = object()
        batch = self._makeOne(table)
        batch._timestamp = timestamp = object()
        row_key = 'row-key'
        batch._row_map[row_key] = row = _MockRow()

        col1_fam = 'cf1'
        col1_qual = 'qual1'
        value1 = 'value1'
        col2_fam = 'cf2'
        col2_qual = 'qual2'
        value2 = 'value2'
        data = {col1_fam + ':' + col1_qual: value1,
                col2_fam + ':' + col2_qual: value2}

        self.assertEqual(batch._mutation_count, 0)
        self.assertEqual(row.set_cell_calls, [])
        batch.put(row_key, data)
        self.assertEqual(batch._mutation_count, 2)
        # Since the calls depend on data.keys(), the order
        # is non-deterministic.
        first_elt = operator.itemgetter(0)
        ordered_calls = sorted(row.set_cell_calls, key=first_elt)

        cell1_args = (col1_fam, col1_qual, value1)
        cell1_kwargs = {'timestamp': timestamp}
        cell2_args = (col2_fam, col2_qual, value2)
        cell2_kwargs = {'timestamp': timestamp}
        self.assertEqual(ordered_calls, [
            (cell1_args, cell1_kwargs),
            (cell2_args, cell2_kwargs),
        ])

    def test_put_call_try_send(self):
        klass = self._getTargetClass()

        class CallTrySend(klass):

            try_send_calls = 0

            def _try_send(self):
                self.try_send_calls += 1

        table = object()
        batch = CallTrySend(table)

        row_key = 'row-key'
        batch._row_map[row_key] = _MockRow()

        self.assertEqual(batch._mutation_count, 0)
        self.assertEqual(batch.try_send_calls, 0)
        # No data so that nothing happens
        batch.put(row_key, data={})
        self.assertEqual(batch._mutation_count, 0)
        self.assertEqual(batch.try_send_calls, 1)

    def test__delete_columns(self):
        table = object()
        time_range = object()
        batch = self._makeOne(table)
        batch._delete_range = time_range

        col1_fam = 'cf1'
        col2_fam = 'cf2'
        col2_qual = 'col-name'
        columns = [col1_fam + ':', col2_fam + ':' + col2_qual]
        row_object = _MockRow()

        batch._delete_columns(columns, row_object)
        self.assertEqual(row_object.commits, 0)

        cell_deleted_args = (col2_fam, col2_qual)
        cell_deleted_kwargs = {'time_range': time_range}
        self.assertEqual(row_object.delete_cell_calls,
                         [(cell_deleted_args, cell_deleted_kwargs)])
        fam_deleted_args = (col1_fam,)
        fam_deleted_kwargs = {'columns': row_object.ALL_COLUMNS}
        self.assertEqual(row_object.delete_cells_calls,
                         [(fam_deleted_args, fam_deleted_kwargs)])

    def test_delete_bad_wal(self):
        from gcloud_bigtable.happybase.batch import _WAL_SENTINEL

        table = object()
        batch = self._makeOne(table)

        row = 'row-key'
        columns = []
        wal = None

        self.assertNotEqual(wal, _WAL_SENTINEL)
        with self.assertRaises(ValueError):
            batch.delete(row, columns=columns, wal=wal)

    def test_delete_entire_row(self):
        table = object()
        batch = self._makeOne(table)

        row_key = 'row-key'
        batch._row_map[row_key] = row = _MockRow()

        self.assertEqual(row.deletes, 0)
        self.assertEqual(batch._mutation_count, 0)
        batch.delete(row_key, columns=None)
        self.assertEqual(row.deletes, 1)
        self.assertEqual(batch._mutation_count, 1)

    def test_delete_call_try_send(self):
        klass = self._getTargetClass()

        class CallTrySend(klass):

            try_send_calls = 0

            def _try_send(self):
                self.try_send_calls += 1

        table = object()
        batch = CallTrySend(table)

        row_key = 'row-key'
        batch._row_map[row_key] = _MockRow()

        self.assertEqual(batch._mutation_count, 0)
        self.assertEqual(batch.try_send_calls, 0)
        # No columns so that nothing happens
        batch.delete(row_key, columns=[])
        self.assertEqual(batch._mutation_count, 0)
        self.assertEqual(batch.try_send_calls, 1)

    def test_delete_some_columns(self):
        table = object()
        batch = self._makeOne(table)

        row_key = 'row-key'
        batch._row_map[row_key] = row = _MockRow()

        self.assertEqual(batch._mutation_count, 0)

        col1_fam = 'cf1'
        col2_fam = 'cf2'
        col2_qual = 'col-name'
        columns = [col1_fam + ':', col2_fam + ':' + col2_qual]
        batch.delete(row_key, columns=columns)

        self.assertEqual(batch._mutation_count, 2)
        cell_deleted_args = (col2_fam, col2_qual)
        cell_deleted_kwargs = {'time_range': None}
        self.assertEqual(row.delete_cell_calls,
                         [(cell_deleted_args, cell_deleted_kwargs)])
        fam_deleted_args = (col1_fam,)
        fam_deleted_kwargs = {'columns': row.ALL_COLUMNS}
        self.assertEqual(row.delete_cells_calls,
                         [(fam_deleted_args, fam_deleted_kwargs)])

    def test_context_manager(self):
        klass = self._getTargetClass()

        class BatchWithSend(_SendMixin, klass):
            pass

        table = object()
        batch = BatchWithSend(table)
        self.assertFalse(batch._send_called)

        with batch:
            pass

        self.assertTrue(batch._send_called)

    def test_context_manager_with_exception_non_transactional(self):
        klass = self._getTargetClass()

        class BatchWithSend(_SendMixin, klass):
            pass

        table = object()
        batch = BatchWithSend(table)
        self.assertFalse(batch._send_called)

        with self.assertRaises(ValueError):
            with batch:
                raise ValueError('Something bad happened')

        self.assertTrue(batch._send_called)

    def test_context_manager_with_exception_transactional(self):
        klass = self._getTargetClass()

        class BatchWithSend(_SendMixin, klass):
            pass

        table = object()
        batch = BatchWithSend(table, transaction=True)
        self.assertFalse(batch._send_called)

        with self.assertRaises(ValueError):
            with batch:
                raise ValueError('Something bad happened')

        self.assertFalse(batch._send_called)

        # Just to make sure send() actually works (and to make cover happy).
        batch.send()
        self.assertTrue(batch._send_called)


class _MockRowMap(dict):

    clear_count = 0

    def clear(self):
        self.clear_count += 1
        super(_MockRowMap, self).clear()


class _MockRow(object):

    ALL_COLUMNS = object()

    def __init__(self):
        self.commits = 0
        self.deletes = 0
        self.set_cell_calls = []
        self.delete_cell_calls = []
        self.delete_cells_calls = []

    def commit(self):
        self.commits += 1

    def delete(self):
        self.deletes += 1

    def set_cell(self, *args, **kwargs):
        self.set_cell_calls.append((args, kwargs))

    def delete_cell(self, *args, **kwargs):
        self.delete_cell_calls.append((args, kwargs))

    def delete_cells(self, *args, **kwargs):
        self.delete_cells_calls.append((args, kwargs))


class _MockConnection(object):

    def __init__(self, cluster):
        self._cluster = cluster


class _MockTable(object):

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection


class _MockLowLevelTable(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.rows_made = []
        self.mock_row = None

    def row(self, row_key):
        self.rows_made.append(row_key)
        return self.mock_row
