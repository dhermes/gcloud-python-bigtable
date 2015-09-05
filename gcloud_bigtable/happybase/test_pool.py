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
        if 'cluster' not in kwargs:
            kwargs['cluster'] = _Cluster()
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
        cluster_copy1 = object()
        cluster_copy2 = object()
        cluster_copy3 = object()
        cluster = _Cluster(cluster_copy1, cluster_copy2, cluster_copy3)
        connection = ConnectionWithOpen(autoconnect=False, cluster=cluster)
        self.assertFalse(connection._open_called)
        self.assertTrue(connection._cluster is cluster_copy1)
        connection = ConnectionWithOpen(autoconnect=True, cluster=cluster)
        self.assertTrue(connection._open_called)
        self.assertTrue(connection._cluster is cluster_copy2)

        # Then make sure autoconnect=True is ignored in a pool.
        size = 1
        with _Monkey(MUT, Connection=ConnectionWithOpen):
            pool = self._makeOne(size, autoconnect=True, cluster=cluster)

        for connection in pool._queue.queue:
            self.assertTrue(isinstance(connection, ConnectionWithOpen))
            self.assertTrue(connection._cluster is cluster_copy3)
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

    def test_connection(self):
        size = 1
        timeout = 10
        pool = self._makeOne(size)
        with self.assertRaises(NotImplementedError):
            pool.connection(timeout=timeout)


class _Cluster(object):

    def __init__(self, *copies):
        self.copies = list(copies)

    def copy(self):
        if self.copies:
            result = self.copies[0]
            self.copies[:] = self.copies[1:]
            return result
        else:
            return self
