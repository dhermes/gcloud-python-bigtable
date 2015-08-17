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

    def test_from_pb(self):
        import datetime
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._helpers import EPOCH

        timestamp_micros = 18738724000  # Make sure millis granularity
        timestamp = EPOCH + datetime.timedelta(microseconds=timestamp_micros)
        value = b'value-bytes'
        cell_expected = self._makeOne(value, timestamp)

        cell_pb = data_pb2.Cell(value=value, timestamp_micros=timestamp_micros)
        klass = self._getTargetClass()
        result = klass.from_pb(cell_pb)
        self.assertEqual(result, cell_expected)

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
        self.assertEqual(partial_row_data._cells, {})
        self.assertFalse(partial_row_data._committed)

    def test_cells_property(self):
        partial_row_data = self._makeOne()
        cells = {1: 2}
        partial_row_data._cells = cells
        # Make sure we get a copy, not the original.
        self.assertFalse(partial_row_data.cells is cells)
        self.assertEqual(partial_row_data.cells, cells)

    def test_clear(self):
        partial_row_data = self._makeOne()
        cells = {1: 2}
        partial_row_data._cells = cells
        self.assertEqual(partial_row_data.cells, cells)
        partial_row_data._committed = True
        partial_row_data.clear()
        self.assertFalse(partial_row_data._committed)
        self.assertEqual(partial_row_data.cells, {})

    def test__handle_commit_row(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()
        chunk = messages_pb2.ReadRowsResponse.Chunk(commit_row=True)

        index = last_chunk_index = 1
        self.assertFalse(partial_row_data._committed)
        partial_row_data._handle_commit_row(chunk, index, last_chunk_index)
        self.assertTrue(partial_row_data._committed)

    def test__handle_commit_row_false(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()
        chunk = messages_pb2.ReadRowsResponse.Chunk(commit_row=False)

        with self.assertRaises(ValueError):
            partial_row_data._handle_commit_row(chunk, None, None)

    def test__handle_commit_row_not_last_chunk(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()
        chunk = messages_pb2.ReadRowsResponse.Chunk(commit_row=True)

        with self.assertRaises(ValueError):
            index = 0
            last_chunk_index = 1
            self.assertNotEqual(index, last_chunk_index)
            partial_row_data._handle_commit_row(chunk, index, last_chunk_index)

    def test__handle_reset_row(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()
        chunk = messages_pb2.ReadRowsResponse.Chunk(reset_row=True)

        # Modify the PartialRowData object so we can check it's been cleared.
        partial_row_data._cells = {1: 2}
        partial_row_data._committed = True
        partial_row_data._handle_reset_row(chunk)
        self.assertEqual(partial_row_data.cells, {})
        self.assertFalse(partial_row_data._committed)

    def test__handle_reset_row_failure(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()
        chunk = messages_pb2.ReadRowsResponse.Chunk(reset_row=False)

        with self.assertRaises(ValueError):
            partial_row_data._handle_reset_row(chunk)

    def test__handle_row_contents(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable.row_data import Cell

        partial_row_data = self._makeOne()
        cell1_pb = data_pb2.Cell(timestamp_micros=1, value=b'val1')
        cell2_pb = data_pb2.Cell(timestamp_micros=200, value=b'val2')
        cell3_pb = data_pb2.Cell(timestamp_micros=300000, value=b'val3')
        col1 = b'col1'
        col2 = b'col2'
        columns = [
            data_pb2.Column(qualifier=col1, cells=[cell1_pb, cell2_pb]),
            data_pb2.Column(qualifier=col2, cells=[cell3_pb]),
        ]
        family_name = u'name'
        row_contents = data_pb2.Family(name=family_name, columns=columns)
        chunk = messages_pb2.ReadRowsResponse.Chunk(row_contents=row_contents)

        self.assertEqual(partial_row_data.cells, {})
        partial_row_data._handle_row_contents(chunk)
        expected_cells = {
            family_name: {
                col1: [Cell.from_pb(cell1_pb), Cell.from_pb(cell2_pb)],
                col2: [Cell.from_pb(cell3_pb)],
            }
        }
        self.assertEqual(partial_row_data.cells, expected_cells)

    def test_update_from_read_rows(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()

        # Set-up chunk1, some data that will be reset by chunk2.
        ignored_family_name = u'ignore-name'
        row_contents = data_pb2.Family(name=ignored_family_name)
        chunk1 = messages_pb2.ReadRowsResponse.Chunk(row_contents=row_contents)

        # Set-up chunk2, a reset row.
        chunk2 = messages_pb2.ReadRowsResponse.Chunk(reset_row=True)

        # Set-up chunk3, a column family with no columns.
        family_name = u'name'
        row_contents = data_pb2.Family(name=family_name)
        chunk3 = messages_pb2.ReadRowsResponse.Chunk(row_contents=row_contents)

        # Set-up chunk4, a commit row.
        chunk4 = messages_pb2.ReadRowsResponse.Chunk(commit_row=True)

        # Prepare request and make sure PartialRowData is empty before.
        read_rows_response_pb = messages_pb2.ReadRowsResponse(
            chunks=[chunk1, chunk2, chunk3, chunk4])
        self.assertEqual(partial_row_data.cells, {})
        self.assertFalse(partial_row_data._committed)

        # Parse the response and make sure the cells took place.
        partial_row_data.update_from_read_rows(read_rows_response_pb)
        self.assertEqual(partial_row_data.cells, {family_name: {}})
        self.assertFalse(ignored_family_name in partial_row_data.cells)
        self.assertTrue(partial_row_data._committed)

    def test_update_from_read_rows_while_committed(self):
        partial_row_data = self._makeOne()
        partial_row_data._committed = True

        with self.assertRaises(ValueError):
            partial_row_data.update_from_read_rows(None)

    def test_update_from_read_rows_empty_chunk(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        partial_row_data = self._makeOne()
        chunk = messages_pb2.ReadRowsResponse.Chunk()
        read_rows_response_pb = messages_pb2.ReadRowsResponse(chunks=[chunk])

        # This makes it an "empty" chunk.
        self.assertEqual(chunk.WhichOneof('chunk'), None)
        with self.assertRaises(ValueError):
            partial_row_data.update_from_read_rows(read_rows_response_pb)

    def test_from_read_rows(self):
        klass = self._getTargetClass()

        class FakePartial(klass):

            def __init__(self):
                self._called = []

            def update_from_read_rows(self, value):
                self._called.append(value)

        fake_input = object()
        result = FakePartial.from_read_rows(fake_input)
        self.assertTrue(isinstance(result, FakePartial))
        self.assertEqual(result._called, [fake_input])
