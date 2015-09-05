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
        client = cluster.client
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


class Test__parse_family_option(unittest2.TestCase):

    def _callFUT(self, option):
        from gcloud_bigtable.happybase.connection import _parse_family_option
        return _parse_family_option(option)

    def test_dictionary_no_keys(self):
        option = {}
        result = self._callFUT(option)
        self.assertEqual(result, None)

    def test_dictionary_bad_key(self):
        option = {'badkey': None}
        with self.assertRaises(ValueError):
            self._callFUT(option)

    def test_dictionary_versions_key(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule

        versions = 42
        option = {'max_versions': versions}
        result = self._callFUT(option)

        gc_rule = GarbageCollectionRule(max_num_versions=versions)
        self.assertEqual(result, gc_rule)

    def test_dictionary_ttl_key(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule

        time_to_live = 24 * 60 * 60
        max_age = datetime.timedelta(days=1)
        option = {'time_to_live': time_to_live}
        result = self._callFUT(option)

        gc_rule = GarbageCollectionRule(max_age=max_age)
        self.assertEqual(result, gc_rule)

    def test_dictionary_both_keys(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)

        versions = 42
        time_to_live = 24 * 60 * 60
        option = {
            'max_versions': versions,
            'time_to_live': time_to_live,
        }
        result = self._callFUT(option)

        max_age = datetime.timedelta(days=1)
        # NOTE: This relies on the order of the rules in the method we are
        #       calling matching this order here.
        gc_rule1 = GarbageCollectionRule(max_age=max_age)
        gc_rule2 = GarbageCollectionRule(max_num_versions=versions)
        gc_rule = GarbageCollectionRuleIntersection(
            rules=[gc_rule1, gc_rule2])
        self.assertEqual(result, gc_rule)

    def test_non_dictionary(self):
        option = object()
        self.assertFalse(isinstance(option, dict))
        result = self._callFUT(option)
        self.assertEqual(result, option)


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
        cluster = _Cluster(copies=[cluster_copy])

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
        from gcloud_bigtable.table import Table

        table_name1 = 'table-name1'
        table_name2 = 'table-name2'
        cluster = _Cluster(list_tables_result=[
            Table(table_name1, None),
            Table(table_name2, None),
        ])
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        result = connection.tables()
        self.assertEqual(result, [table_name1, table_name2])

    def test_tables_with_prefix(self):
        from gcloud_bigtable.table import Table

        table_prefix = 'prefix'
        table_prefix_separator = '<>'
        unprefixed_table_name1 = 'table-name1'

        table_name1 = (table_prefix + table_prefix_separator +
                       unprefixed_table_name1)
        table_name2 = 'table-name2'
        cluster = _Cluster(list_tables_result=[
            Table(table_name1, None),
            Table(table_name2, None),
        ])
        connection = self._makeOne(
            autoconnect=False, cluster=cluster, table_prefix=table_prefix,
            table_prefix_separator=table_prefix_separator)
        result = connection.tables()
        self.assertEqual(result, [unprefixed_table_name1])

    def test_create_table(self):
        import operator
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import connection as MUT

        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        mock_gc_rule = object()
        mock_parse_family_option = _MockCalled(mock_gc_rule)

        name = 'table-name'
        col_fam1 = 'cf1'
        col_fam_option1 = object()
        col_fam2 = 'cf2'
        col_fam_option2 = object()
        families = {
            col_fam1: col_fam_option1,
            # A trailing colon is also allowed.
            col_fam2 + ':': col_fam_option2,
        }
        table_instances = []
        col_fam_instances = []
        with _Monkey(MUT, _LowLevelTable=_MockLowLevelTable,
                     _parse_family_option=mock_parse_family_option):
            _MockLowLevelTable._instances = table_instances
            _MockLowLevelColumnFamily._instances = col_fam_instances
            connection.create_table(name, families)

        # Just one table would have been created.
        table_instance, = table_instances
        self.assertEqual(table_instance.args, ('table-name', cluster))
        self.assertEqual(table_instance.kwargs, {})
        self.assertEqual(table_instance.create_calls, 1)

        # Check if our mock was called twice, but we don't know the order.
        mock_called = mock_parse_family_option.called_args
        self.assertEqual(len(mock_called), 2)
        self.assertEqual(map(len, mock_called), [1, 1])
        self.assertEqual(set(mock_called[0] + mock_called[1]),
                         set([col_fam_option1, col_fam_option2]))

        # We expect two column family instances created, but don't know the
        # order due to non-deterministic dict.items().
        col_fam_instances.sort(key=operator.attrgetter('column_family_id'))
        self.assertEqual(col_fam_instances[0].column_family_id, col_fam1)
        self.assertEqual(col_fam_instances[0].gc_rule, mock_gc_rule)
        self.assertEqual(col_fam_instances[0].create_calls, 1)
        self.assertEqual(col_fam_instances[1].column_family_id, col_fam2)
        self.assertEqual(col_fam_instances[1].gc_rule, mock_gc_rule)
        self.assertEqual(col_fam_instances[1].create_calls, 1)

    def test_delete_table(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.happybase import connection as MUT

        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)

        name = 'table-name'
        instances = []
        with _Monkey(MUT, _LowLevelTable=_MockLowLevelTable):
            _MockLowLevelTable._instances = instances
            connection.delete_table(name)

        # Just one table would have been created.
        table_instance, = instances
        self.assertEqual(table_instance.args, ('table-name', cluster))
        self.assertEqual(table_instance.kwargs, {})
        self.assertEqual(table_instance.delete_calls, 1)

    def test_delete_table_disable(self):
        cluster = _Cluster()  # Avoid implicit environ check.
        connection = self._makeOne(autoconnect=False, cluster=cluster)
        name = 'table-name'
        with self.assertRaises(ValueError):
            connection.delete_table(name, disable=True)

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
            cluster.client = self
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

    def __init__(self, copies=(), list_tables_result=()):
        self.copies = list(copies)
        # Included to support Connection.__del__
        self.client = _Client()
        self.list_tables_result = list_tables_result

    def copy(self):
        if self.copies:
            result = self.copies[0]
            self.copies[:] = self.copies[1:]
            return result
        else:
            return self

    def list_tables(self):
        return self.list_tables_result


class _MockLowLevelTable(object):

    _instances = []

    def __init__(self, *args, **kwargs):
        self._instances.append(self)
        self.args = args
        self.kwargs = kwargs
        self.delete_calls = 0
        self.create_calls = 0

    def delete(self):
        self.delete_calls += 1

    def create(self):
        self.create_calls += 1

    def column_family(self, column_family_id, gc_rule=None):
        return _MockLowLevelColumnFamily(column_family_id, gc_rule=gc_rule)


class _MockLowLevelColumnFamily(object):

    _instances = []

    def __init__(self, column_family_id, gc_rule=None):
        self._instances.append(self)
        self.column_family_id = column_family_id
        self.gc_rule = gc_rule
        self.create_calls = 0

    def create(self):
        self.create_calls += 1
