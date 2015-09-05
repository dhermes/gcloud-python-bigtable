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


class Test__get_cluster(unittest2.TestCase):

    def _callFUT(self, timeout=None):
        from gcloud_bigtable.happybase.connection import _get_cluster
        return _get_cluster(timeout=timeout)

    def _helper(self, timeout=None, clusters=(), failed_zones=()):
        from functools import partial
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import connection as MUT

        client_with_clusters = partial(_Client, clusters=clusters,
                                       failed_zones=failed_zones)
        with _Monkey(MUT, Client=client_with_clusters):
            result = self._callFUT(timeout=timeout)

        # If we've reached this point, then _callFUT didn't fail, so we know
        # there is exactly one cluster.
        cluster, = clusters
        self.assertEqual(result, cluster)
        client = cluster._client
        self.assertEqual(client.args, ())
        expected_kwargs = {'admin': True}
        if timeout is not None:
            expected_kwargs['timeout_seconds'] = timeout / 1000.0
        self.assertEqual(client.kwargs, expected_kwargs)
        self.assertEqual(client.start_calls, 1)
        self.assertEqual(client.stop_calls, 1)

    def test_default(self):
        cluster = _Cluster()
        self._helper(clusters=[cluster])

    def test_with_timeout(self):
        cluster = _Cluster()
        self._helper(timeout=2103, clusters=[cluster])

    def test_with_no_clusters(self):
        with self.assertRaises(ValueError):
            self._helper()

    def test_with_too_many_clusters(self):
        clusters = [_Cluster(), _Cluster()]
        with self.assertRaises(ValueError):
            self._helper(clusters=clusters)

    def test_with_failed_zones(self):
        cluster = _Cluster()
        failed_zone = 'us-central1-c'
        with self.assertRaises(ValueError):
            self._helper(clusters=[cluster],
                         failed_zones=[failed_zone])


class TestConnection(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.happybase.connection import Connection
        return Connection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        self.assertEqual(cluster.client.start_calls, 0)
        connection = self._makeOne(cluster=cluster)
        self.assertEqual(cluster.client.start_calls, 1)
        self.assertEqual(cluster.client.stop_calls, 0)

        self.assertEqual(connection._cluster, cluster)
        self.assertEqual(connection.table_prefix, None)
        self.assertEqual(connection.table_prefix_separator, '_')

    def test_constructor_no_autoconnect(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        self.assertEqual(connection.table_prefix, None)
        self.assertEqual(connection.table_prefix_separator, '_')

    def test_constructor_missing_cluster(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import connection as MUT

        cluster = _Cluster()
        timeout = object()
        mock_get_cluster = _MockCalled(cluster)
        with _Monkey(MUT, _get_cluster=mock_get_cluster):
            connection = self._makeOne(autoconnect=False, cluster=None,
                                       timeout=timeout)
            self.assertEqual(connection.table_prefix, None)
            self.assertEqual(connection.table_prefix_separator, '_')
            self.assertEqual(connection._cluster, cluster)

        mock_get_cluster.check_called(self, [()], [{'timeout': timeout}])

    def test_constructor_explicit(self):
        timeout = object()
        table_prefix = 'table-prefix'
        table_prefix_separator = 'sep'
        cluster_copy = _Cluster()
        cluster = _Cluster(cluster_copy)

        connection = self._makeOne(
            autoconnect=False, timeout=timeout,
            table_prefix=table_prefix,
            table_prefix_separator=table_prefix_separator,
            cluster=cluster)
        self.assertEqual(connection.table_prefix, table_prefix)
        self.assertEqual(connection.table_prefix_separator,
                         table_prefix_separator)
        self.assertEqual(connection._cluster, cluster_copy)

    def test_constructor_non_string_prefix(self):
        table_prefix = object()

        with self.assertRaises(TypeError):
            self._makeOne(autoconnect=False,
                          table_prefix=table_prefix)

    def test_constructor_non_string_prefix_separator(self):
        table_prefix_separator = object()

        with self.assertRaises(TypeError):
            self._makeOne(autoconnect=False,
                          table_prefix_separator=table_prefix_separator)

    def test_constructor_with_host(self):
        with self.assertRaises(ValueError):
            self._makeOne(host=object())

    def test_constructor_with_port(self):
        with self.assertRaises(ValueError):
            self._makeOne(port=object())

    def test_constructor_with_compat(self):
        with self.assertRaises(ValueError):
            self._makeOne(compat=object())

    def test_constructor_with_transport(self):
        with self.assertRaises(ValueError):
            self._makeOne(transport=object())

    def test_constructor_with_protocol(self):
        with self.assertRaises(ValueError):
            self._makeOne(protocol=object())

    def test_open(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        self.assertEqual(cluster.client.start_calls, 0)
        connection.open()
        self.assertEqual(cluster.client.start_calls, 1)
        self.assertEqual(cluster.client.stop_calls, 0)

    def test_close(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        self.assertEqual(cluster.client.stop_calls, 0)
        connection.close()
        self.assertEqual(cluster.client.stop_calls, 1)
        self.assertEqual(cluster.client.start_calls, 0)

    def test___del__good_initialization(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        self.assertEqual(cluster.client.stop_calls, 0)
        connection.__del__()
        self.assertEqual(cluster.client.stop_calls, 1)

    def test___del__bad_initialization(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        # Fake that initialization failed.
        del connection._initialized

        self.assertEqual(cluster.client.stop_calls, 0)
        connection.__del__()
        self.assertEqual(cluster.client.stop_calls, 0)

    def test__table_name_with_prefix_set(self):
        table_prefix = 'table-prefix'
        table_prefix_separator = '<>'
        cluster = _Cluster()

        connection = self._makeOne(
            autoconnect=False,
            table_prefix=table_prefix,
            table_prefix_separator=table_prefix_separator,
            cluster=cluster)

        name = 'some-name'
        prefixed = connection._table_name(name)
        self.assertEqual(prefixed,
                         table_prefix + table_prefix_separator + name)

    def test__table_name_with_no_prefix_set(self):
        cluster = _Cluster()
        connection = self._makeOne(autoconnect=False,
                                   cluster=cluster)

        name = 'some-name'
        prefixed = connection._table_name(name)
        self.assertEqual(prefixed, name)

    def test_table_factory(self):
        from gcloud_bigtable.happybase.table import Table

        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        table = connection.table(name)

        self.assertTrue(isinstance(table, Table))
        self.assertEqual(table.name, name)
        self.assertEqual(table.connection, connection)

    def _table_factory_prefix_helper(self, use_prefix=True):
        from gcloud_bigtable.happybase.table import Table

        cluster = _Cluster()  # Avoid implicit environ check.
        table_prefix = 'table-prefix'
        table_prefix_separator = '<>'
        connection = self._makeOne(
            autoconnect=False, table_prefix=table_prefix,
            table_prefix_separator=table_prefix_separator,
            cluster=cluster)

        name = 'table-name'
        table = connection.table(name, use_prefix=use_prefix)

        self.assertTrue(isinstance(table, Table))
        prefixed_name = table_prefix + table_prefix_separator + name
        if use_prefix:
            self.assertEqual(table.name, prefixed_name)
        else:
            self.assertEqual(table.name, name)
        self.assertEqual(table.connection, connection)

    def test_table_factory_with_prefix(self):
        self._table_factory_prefix_helper(use_prefix=True)

    def test_table_factory_with_ignored_prefix(self):
        self._table_factory_prefix_helper(use_prefix=False)

    def test_tables(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        with self.assertRaises(NotImplementedError):
            connection.tables()

    def test_create_table(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        families = {}
        with self.assertRaises(NotImplementedError):
            connection.create_table(name, families)

    def test_delete_table(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        disable = True
        with self.assertRaises(NotImplementedError):
            connection.delete_table(name, disable=disable)

    def test_enable_table(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        with self.assertRaises(NotImplementedError):
            connection.enable_table(name)

    def test_disable_table(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        with self.assertRaises(NotImplementedError):
            connection.disable_table(name)

    def test_is_table_enabled(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        with self.assertRaises(NotImplementedError):
            connection.is_table_enabled(name)

    def test_compact_table(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        major = True
        with self.assertRaises(NotImplementedError):
            connection.compact_table(name, major=major)


class _Client(object):

    def __init__(self, *args, **kwargs):
        self.clusters = kwargs.pop('clusters', [])
        for cluster in self.clusters:
            cluster._client = self
        self.failed_zones = kwargs.pop('failed_zones', [])
        self.args = args
        self.kwargs = kwargs
        self.start_calls = 0
        self.stop_calls = 0

    def start(self):
        self.start_calls += 1

    def stop(self):
        self.stop_calls += 1

    def list_clusters(self):
        return self.clusters, self.failed_zones


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
