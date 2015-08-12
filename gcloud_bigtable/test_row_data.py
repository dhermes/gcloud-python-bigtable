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


class TestCell(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row_data import Cell
        return Cell

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        value = object()
        timestamp = object()
        cell = self._makeOne(value, timestamp)
        self.assertEqual(cell.value, value)
        self.assertEqual(cell.timestamp, timestamp)

    def test___eq__(self):
        value = object()
        timestamp = object()
        cell1 = self._makeOne(value, timestamp)
        cell2 = self._makeOne(value, timestamp)
        self.assertEqual(cell1, cell2)

    def test___eq__type_differ(self):
        cell1 = self._makeOne(None, None)
        cell2 = object()
        self.assertNotEqual(cell1, cell2)

    def test___ne__same_value(self):
        value = object()
        timestamp = object()
        cell1 = self._makeOne(value, timestamp)
        cell2 = self._makeOne(value, timestamp)
        comparison_val = (cell1 != cell2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        value1 = 'value1'
        value2 = 'value2'
        timestamp = object()
        cell1 = self._makeOne(value1, timestamp)
        cell2 = self._makeOne(value2, timestamp)
        self.assertNotEqual(cell1, cell2)


class TestPartialRowData(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row_data import PartialRowData
        return PartialRowData

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        partial_row_data = self._makeOne()
        self.assertEqual(partial_row_data._row_key, None)
        self.assertEqual(partial_row_data._cells, [])

    def test_cells_property(self):
        partial_row_data = self._makeOne()
        cells = [1, 2]
        partial_row_data._cells = cells
        # Make sure we get a copy, not the original.
        self.assertFalse(partial_row_data.cells is cells)
        self.assertEqual(partial_row_data.cells, cells)

    def test_clear(self):
        partial_row_data = self._makeOne()
        cells = [1, 2]
        partial_row_data._cells = cells
        self.assertEqual(partial_row_data.cells, cells)
        partial_row_data.clear()
        self.assertEqual(partial_row_data.cells, [])
