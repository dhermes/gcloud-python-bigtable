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
        cluster_copy = _Cluster()
        all_copies = [cluster_copy] * size
        cluster = _Cluster(*all_copies)  # Avoid implicit environ check.
        pool = self._makeOne(size, cluster=cluster)

        self.assertTrue(isinstance(pool._lock, type(threading.Lock())))
        self.assertTrue(isinstance(pool._thread_connections, threading.local))
        self.assertEqual(pool._thread_connections.__dict__, {})

        queue = pool._queue
        self.assertTrue(isinstance(queue, LifoQueue))
        self.assertTrue(queue.full())
        self.assertEqual(queue.maxsize, size)
        for connection in queue.queue:
            self.assertTrue(isinstance(connection, Connection))
            self.assertTrue(connection._cluster is cluster_copy)

    def test_constructor_passes_kwargs(self):
        timeout = 1000
        table_prefix = 'foo'
        table_prefix_separator = '<>'
        cluster = _Cluster()  # Avoid implicit environ check.

        size = 1
        pool = self._makeOne(size, timeout=timeout, table_prefix=table_prefix,
                             table_prefix_separator=table_prefix_separator,
                             cluster=cluster)

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
        cluster_copy1 = _Cluster()
        cluster_copy2 = _Cluster()
        cluster_copy3 = _Cluster()
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

    def test_constructor_infers_cluster(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase.connection import Connection
        from gcloud_bigtable.happybase import pool as MUT

        size = 1
        cluster_copy = _Cluster()
        all_copies = [cluster_copy] * size
        cluster = _Cluster(*all_copies)

        timeout = object()
        mock_get_cluster = _MockCalled(cluster)
        with _Monkey(MUT, _get_cluster=mock_get_cluster):
            pool = self._makeOne(size, timeout=timeout)

        for connection in pool._queue.queue:
            self.assertTrue(isinstance(connection, Connection))
            # We know that the Connection() constructor will
            # call cluster.copy().
            self.assertTrue(connection._cluster is cluster_copy)

        mock_get_cluster.check_called(self, [()], [{'timeout': timeout}])

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
        cluster = _Cluster()  # Avoid implicit environ check.
        pool = self._makeOne(size, cluster=cluster)
        with self.assertRaises(NotImplementedError):
            pool.connection(timeout=timeout)


class _Client(object):

    def __init__(self):
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1


class _Cluster(object):

    def __init__(self, *copies):
        self.copies = list(copies)
        # Included to support Connection.__del__
        self.client = _Client()

    def copy(self):
        if self.copies:
            result = self.copies[0]
            self.copies[:] = self.copies[1:]
            return result
        else:
            return self
