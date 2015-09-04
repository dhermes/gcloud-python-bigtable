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


class TestConnectionPool(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.happybase.pool import ConnectionPool
        return ConnectionPool

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        from Queue import LifoQueue
        import threading
        from gcloud_bigtable.happybase.connection import Connection

        size = 11
        pool = self._makeOne(size)

        self.assertTrue(isinstance(pool._lock, type(threading.Lock())))
        self.assertTrue(isinstance(pool._thread_connections, threading.local))
        self.assertEqual(pool._thread_connections.__dict__, {})

        queue = pool._queue
        self.assertTrue(isinstance(queue, LifoQueue))
        self.assertTrue(queue.full())
        self.assertEqual(queue.maxsize, size)
        for connection in queue.queue:
            self.assertTrue(isinstance(connection, Connection))

    def test_constructor_passes_kwargs(self):
        timeout = 1000
        table_prefix = 'foo'
        table_prefix_separator = '<>'

        size = 1
        pool = self._makeOne(size, timeout=timeout, table_prefix=table_prefix,
                             table_prefix_separator=table_prefix_separator)

        for connection in pool._queue.queue:
            self.assertEqual(connection.timeout, timeout)
            self.assertEqual(connection.table_prefix, table_prefix)
            self.assertEqual(connection.table_prefix_separator,
                             table_prefix_separator)

    def test_constructor_ignores_autoconnect(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase.connection import Connection
        from gcloud_bigtable.happybase import pool as MUT

        class ConnectionWithOpen(Connection):

            _open_called = False

            def open(self):
                self._open_called = True

        # First make sure the custom Connection class does as expected.
        connection = ConnectionWithOpen(autoconnect=False)
        self.assertFalse(connection._open_called)
        connection = ConnectionWithOpen(autoconnect=True)
        self.assertTrue(connection._open_called)

        # Then make sure autoconnect=True is ignored in a pool.
        size = 1
        with _Monkey(MUT, Connection=ConnectionWithOpen):
            pool = self._makeOne(size, autoconnect=True)

        for connection in pool._queue.queue:
            self.assertTrue(isinstance(connection, ConnectionWithOpen))
            self.assertFalse(connection._open_called)

    def test_constructor_non_integer_size(self):
        size = None
        with self.assertRaises(TypeError):
            self._makeOne(size)

    def test_constructor_non_positive_size(self):
        size = -10
        with self.assertRaises(ValueError):
            self._makeOne(size)
        size = 0
        with self.assertRaises(ValueError):
            self._makeOne(size)
