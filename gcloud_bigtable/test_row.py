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
