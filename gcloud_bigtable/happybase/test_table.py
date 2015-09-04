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
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        with self.assertRaises(NotImplementedError):
            table.families()

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
        name = 'table-name'
        connection = object()
        table = self._makeOne(name, connection)

        timestamp = object()
        batch_size = 42
        transaction = False  # Must be False when batch_size is non-null
        with self.assertRaises(NotImplementedError):
            table.batch(timestamp=timestamp, batch_size=batch_size,
                        transaction=transaction)
