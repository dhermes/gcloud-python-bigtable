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
