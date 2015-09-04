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
        self.assertEqual(batch._transaction, False)

    def test_constructor_explicit(self):
        table = object()
        timestamp = object()
        batch_size = 42
        transaction = False  # Must be False when batch_size is non-null

        batch = self._makeOne(table, timestamp=timestamp,
                              batch_size=batch_size, transaction=transaction)
        self.assertEqual(batch._table, table)
        self.assertEqual(batch._batch_size, batch_size)
        self.assertEqual(batch._timestamp, timestamp)
        self.assertEqual(batch._transaction, transaction)

    def test_constructor_with_non_default_wal(self):
        table = object()
        wal = object()
        with self.assertRaises(ValueError):
            self._makeOne(table, wal=wal)

    def test_constructor_with_negative_batch_size(self):
        table = object()
        batch_size = -1
        with self.assertRaises(ValueError):
            self._makeOne(table, batch_size=batch_size)

    def test_constructor_with_batch_size_and_transactional(self):
        table = object()
        batch_size = 1
        transaction = True
        with self.assertRaises(TypeError):
            self._makeOne(table, batch_size=batch_size,
                          transaction=transaction)
