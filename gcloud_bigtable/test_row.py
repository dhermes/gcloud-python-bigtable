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


class TestRow(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import Row
        return Row

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _constructor_helper(self, row_key, row_key_expected=None):
        table = object()
        row = self._makeOne(row_key, table)
        row_key_val = row_key_expected or row_key
        # Only necessary in Py2
        self.assertEqual(type(row._row_key), type(row_key_val))
        self.assertEqual(row._row_key, row_key_val)
        self.assertTrue(row._table is table)
        self.assertEqual(row._pb_mutations, [])

    def test_constructor(self):
        row_key = b'row_key'
        self._constructor_helper(row_key)

    def test_constructor_with_unicode(self):
        row_key = u'row_key'
        row_key_expected = b'row_key'
        self._constructor_helper(row_key, row_key_expected=row_key_expected)

    def test_constructor_with_non_bytes(self):
        row_key = object()
        with self.assertRaises(TypeError):
            self._constructor_helper(row_key)

    def test_table_getter(self):
        table = object()
        row = self._makeOne(b'row_key', table)
        self.assertTrue(row.table is table)

    def test_row_key_getter(self):
        row_key = b'row_key'
        row = self._makeOne(row_key, object())
        self.assertEqual(row.row_key, row_key)

    def test_delete(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        row = self._makeOne(b'row_key', object())
        self.assertEqual(row._pb_mutations, [])
        row.delete()

        expected_pb = data_pb2.Mutation(
            delete_from_row=data_pb2.Mutation.DeleteFromRow(),
        )
        self.assertEqual(row._pb_mutations, [expected_pb])

    def _set_cell_helper(self, column=b'column',
                         column_bytes=None, timestamp=None,
                         timestamp_micros=-1):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(b'row_key', table)
        column_family_id = u'column_family_id'
        value = b'foobar'
        row.set_cell(column_family_id, column,
                     value, timestamp=timestamp)

        expected_pb = data_pb2.Mutation(
            set_cell=data_pb2.Mutation.SetCell(
                family_name=column_family_id,
                column_qualifier=column_bytes or column,
                timestamp_micros=timestamp_micros,
                value=value,
            ),
        )
        self.assertEqual(row._pb_mutations, [expected_pb])

    def test_set_cell(self):
        column = b'column'
        self._set_cell_helper(column=column)

    def test_set_cell_with_string_column(self):
        column = u'column'
        column_bytes = b'column'
        self._set_cell_helper(column=column,
                              column_bytes=column_bytes)

    def test_set_cell_with_non_bytes_value(self):
        table = object()
        row = self._makeOne(b'row_key', table)
        value = object()  # Not bytes
        with self.assertRaises(TypeError):
            row.set_cell(None, None, value)

    def test_set_cell_with_non_null_timestamp(self):
        import datetime
        from gcloud_bigtable import row as MUT

        microseconds = 898294371
        millis_granularity = microseconds - (microseconds % 1000)
        timestamp = MUT._EPOCH + datetime.timedelta(microseconds=microseconds)
        self._set_cell_helper(timestamp=timestamp,
                              timestamp_micros=millis_granularity)

    def test_set_cell_with_non_utc_timestamp(self):
        import datetime

        microseconds = 0
        epoch_no_tz = datetime.datetime.utcfromtimestamp(0)
        with self.assertRaises(TypeError):
            self._set_cell_helper(timestamp=epoch_no_tz,
                                  timestamp_micros=microseconds)

    def test_set_cell_with_non_datetime_timestamp(self):
        timestamp = object()  # Not a datetime object.
        with self.assertRaises(TypeError):
            self._set_cell_helper(timestamp=timestamp)
