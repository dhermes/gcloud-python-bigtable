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

from gcloud_bigtable._grpc_mocks import GRPCMockTestMixin


PROJECT_ID = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'
TABLE_ID = 'table-id'


class TestGarbageCollectionRule(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.table import GarbageCollectionRule
        return GarbageCollectionRule

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        gc_rule = self._makeOne()
        self.assertEqual(gc_rule.max_num_versions, None)
        self.assertEqual(gc_rule.max_age, None)

    def test_constructor_failure(self):
        with self.assertRaises(ValueError):
            self._makeOne(max_num_versions=1, max_age=object())

    def test_to_pb_no_value(self):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        gc_rule = self._makeOne()
        pb_val = gc_rule.to_pb()
        self.assertEqual(pb_val, data_pb2.GcRule())

    def test_to_pb_with_max_num_versions(self):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        max_num_versions = 1337
        gc_rule = self._makeOne(max_num_versions=max_num_versions)
        pb_val = gc_rule.to_pb()
        self.assertEqual(pb_val,
                         data_pb2.GcRule(max_num_versions=max_num_versions))

    def test_to_pb_with_max_age(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2

        max_age = datetime.timedelta(seconds=1)
        duration = duration_pb2.Duration(seconds=1)
        gc_rule = self._makeOne(max_age=max_age)
        pb_val = gc_rule.to_pb()
        self.assertEqual(pb_val, data_pb2.GcRule(max_age=duration))


class TestGarbageCollectionRuleUnion(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.table import GarbageCollectionRuleUnion
        return GarbageCollectionRuleUnion

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        rules = object()
        rule_union = self._makeOne(rules=rules)
        self.assertTrue(rule_union.rules is rules)

    def test_to_pb(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2
        from gcloud_bigtable.table import GarbageCollectionRule

        max_num_versions = 42
        rule1 = GarbageCollectionRule(max_num_versions=max_num_versions)
        pb_rule1 = data_pb2.GcRule(max_num_versions=max_num_versions)

        max_age = datetime.timedelta(seconds=1)
        rule2 = GarbageCollectionRule(max_age=max_age)
        pb_rule2 = data_pb2.GcRule(max_age=duration_pb2.Duration(seconds=1))

        rule3 = self._makeOne(rules=[rule1, rule2])
        pb_rule3 = data_pb2.GcRule(
            union=data_pb2.GcRule.Union(rules=[pb_rule1, pb_rule2]))

        gc_rule_pb = rule3.to_pb()
        self.assertEqual(gc_rule_pb, pb_rule3)

    def test_to_pb_nested(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2
        from gcloud_bigtable.table import GarbageCollectionRule

        max_num_versions1 = 42
        rule1 = GarbageCollectionRule(max_num_versions=max_num_versions1)
        pb_rule1 = data_pb2.GcRule(max_num_versions=max_num_versions1)

        max_age = datetime.timedelta(seconds=1)
        rule2 = GarbageCollectionRule(max_age=max_age)
        pb_rule2 = data_pb2.GcRule(max_age=duration_pb2.Duration(seconds=1))

        rule3 = self._makeOne(rules=[rule1, rule2])
        pb_rule3 = data_pb2.GcRule(
            union=data_pb2.GcRule.Union(rules=[pb_rule1, pb_rule2]))

        max_num_versions2 = 1337
        rule4 = GarbageCollectionRule(max_num_versions=max_num_versions2)
        pb_rule4 = data_pb2.GcRule(max_num_versions=max_num_versions2)

        rule5 = self._makeOne(rules=[rule3, rule4])
        pb_rule5 = data_pb2.GcRule(
            union=data_pb2.GcRule.Union(rules=[pb_rule3, pb_rule4]))

        gc_rule_pb = rule5.to_pb()
        self.assertEqual(gc_rule_pb, pb_rule5)


class TestGarbageCollectionRuleIntersection(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.table import GarbageCollectionRuleIntersection
        return GarbageCollectionRuleIntersection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        rules = object()
        rule_intersection = self._makeOne(rules=rules)
        self.assertTrue(rule_intersection.rules is rules)

    def test_to_pb(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2
        from gcloud_bigtable.table import GarbageCollectionRule

        max_num_versions = 42
        rule1 = GarbageCollectionRule(max_num_versions=max_num_versions)
        pb_rule1 = data_pb2.GcRule(max_num_versions=max_num_versions)

        max_age = datetime.timedelta(seconds=1)
        rule2 = GarbageCollectionRule(max_age=max_age)
        pb_rule2 = data_pb2.GcRule(max_age=duration_pb2.Duration(seconds=1))

        rule3 = self._makeOne(rules=[rule1, rule2])
        pb_rule3 = data_pb2.GcRule(
            intersection=data_pb2.GcRule.Intersection(
                rules=[pb_rule1, pb_rule2]))

        gc_rule_pb = rule3.to_pb()
        self.assertEqual(gc_rule_pb, pb_rule3)

    def test_to_pb_nested(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2
        from gcloud_bigtable.table import GarbageCollectionRule

        max_num_versions1 = 42
        rule1 = GarbageCollectionRule(max_num_versions=max_num_versions1)
        pb_rule1 = data_pb2.GcRule(max_num_versions=max_num_versions1)

        max_age = datetime.timedelta(seconds=1)
        rule2 = GarbageCollectionRule(max_age=max_age)
        pb_rule2 = data_pb2.GcRule(max_age=duration_pb2.Duration(seconds=1))

        rule3 = self._makeOne(rules=[rule1, rule2])
        pb_rule3 = data_pb2.GcRule(
            intersection=data_pb2.GcRule.Intersection(
                rules=[pb_rule1, pb_rule2]))

        max_num_versions2 = 1337
        rule4 = GarbageCollectionRule(max_num_versions=max_num_versions2)
        pb_rule4 = data_pb2.GcRule(max_num_versions=max_num_versions2)

        rule5 = self._makeOne(rules=[rule3, rule4])
        pb_rule5 = data_pb2.GcRule(
            intersection=data_pb2.GcRule.Intersection(
                rules=[pb_rule3, pb_rule4]))

        gc_rule_pb = rule5.to_pb()
        self.assertEqual(gc_rule_pb, pb_rule5)


class TestTable(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import client
        from gcloud_bigtable import table as MUT
        cls._MUT = MUT
        cls._STUB_SCOPES = [client.DATA_SCOPE]
        cls._STUB_FACTORY_NAME = 'TABLE_STUB_FACTORY'
        cls._STUB_HOST = MUT.TABLE_ADMIN_HOST
        cls._STUB_PORT = MUT.TABLE_ADMIN_PORT

    @classmethod
    def tearDownClass(cls):
        del cls._MUT
        del cls._STUB_SCOPES
        del cls._STUB_FACTORY_NAME
        del cls._STUB_HOST
        del cls._STUB_PORT

    def _getTargetClass(self):
        from gcloud_bigtable.table import Table
        return Table

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        cluster = object()
        table = self._makeOne(TABLE_ID, cluster)
        self.assertEqual(table.table_id, TABLE_ID)
        self.assertTrue(table._cluster is cluster)

    def test_cluster_getter(self):
        cluster = object()
        table = self._makeOne(TABLE_ID, cluster)
        self.assertTrue(table.cluster is cluster)

    def test_name_property(self):
        cluster_name = 'cluster_name'
        cluster = _Cluster(cluster_name)
        table = self._makeOne(TABLE_ID, cluster)
        expected_name = cluster_name + '/tables/' + TABLE_ID
        self.assertEqual(table.name, expected_name)

    def test___eq__(self):
        table_id = 'table_id'
        cluster = object()
        table1 = self._makeOne(table_id, cluster)
        table2 = self._makeOne(table_id, cluster)
        self.assertEqual(table1, table2)

    def test___eq__type_differ(self):
        table1 = self._makeOne('table_id', None)
        table2 = object()
        self.assertNotEqual(table1, table2)

    def test___ne__same_value(self):
        table_id = 'table_id'
        cluster = object()
        table1 = self._makeOne(table_id, cluster)
        table2 = self._makeOne(table_id, cluster)
        comparison_val = (table1 != table2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        table1 = self._makeOne('table_id1', 'cluster1')
        table2 = self._makeOne('table_id2', 'cluster2')
        self.assertNotEqual(table1, table2)

    def test_exists(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.GetTableRequest(name=table_name)

        # Create response_pb
        response_pb = None

        # Create expected_result.
        expected_result = True

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.exists()

        self._grpc_client_test_helper('GetTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def _create_test_helper(self, initial_split_keys):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        # Create request_pb
        cluster_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys,
            name=cluster_name,
            table_id=TABLE_ID,
        )

        # Create response_pb
        response_pb = data_pb2.Table()

        # Create expected_result.
        expected_result = None  # create() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.create(initial_split_keys=initial_split_keys)

        self._grpc_client_test_helper('CreateTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def test_create(self):
        initial_split_keys = None
        self._create_test_helper(initial_split_keys)

    def test_create_with_split_keys(self):
        initial_split_keys = ['s1', 's2']
        self._create_test_helper(initial_split_keys)

    def test_rename(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2

        new_table_id = 'new_table_id'
        self.assertNotEqual(new_table_id, TABLE_ID)

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.RenameTableRequest(
            name=table_name,
            new_id=new_table_id,
        )

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Create expected_result.
        expected_result = None  # rename() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.rename(new_table_id)

        self._grpc_client_test_helper('RenameTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def test_delete(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        request_pb = messages_pb2.DeleteTableRequest(name=table_name)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.delete()

        self._grpc_client_test_helper('DeleteTable', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def _create_column_family_test_helper(self, gc_rule=None):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        # Create request_pb
        column_family_id = 'column_family_id'
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        if gc_rule is None:
            column_family = data_pb2.ColumnFamily()
        else:
            column_family = data_pb2.ColumnFamily(gc_rule=gc_rule.to_pb())
        request_pb = messages_pb2.CreateColumnFamilyRequest(
            name=table_name,
            column_family_id=column_family_id,
            column_family=column_family,
        )

        # Create response_pb
        response_pb = data_pb2.ColumnFamily()

        # Create expected_result.
        expected_result = None  # create_column_family() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.create_column_family(column_family_id,
                                              gc_rule=gc_rule)

        self._grpc_client_test_helper('CreateColumnFamily', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def test_create_column_family(self):
        self._create_column_family_test_helper(gc_rule=None)

    def test_create_column_family_with_gc_rule(self):
        from gcloud_bigtable.table import GarbageCollectionRule
        gc_rule = GarbageCollectionRule(max_num_versions=1337)
        self._create_column_family_test_helper(gc_rule=gc_rule)

    def _update_column_family_test_helper(self, gc_rule=None):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)

        # Create request_pb
        column_family_id = 'column_family_id'
        column_family_name = (
            'projects/' + PROJECT_ID + '/zones/' + ZONE +
            '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID +
            '/columnFamilies/' + column_family_id)
        if gc_rule is None:
            request_pb = data_pb2.ColumnFamily(name=column_family_name)
        else:
            request_pb = data_pb2.ColumnFamily(
                name=column_family_name,
                gc_rule=gc_rule.to_pb(),
            )

        # Create response_pb
        response_pb = data_pb2.ColumnFamily()

        # Create expected_result.
        expected_result = None  # create_column_family() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = TEST_CASE._makeOne(TABLE_ID, cluster)
            return table.update_column_family(column_family_id,
                                              gc_rule=gc_rule)

        self._grpc_client_test_helper('UpdateColumnFamily', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID)

    def test_update_column_family(self):
        self._update_column_family_test_helper(gc_rule=None)

    def test_update_column_family_with_gc_rule(self):
        from gcloud_bigtable.table import GarbageCollectionRule
        gc_rule = GarbageCollectionRule(max_num_versions=1337)
        self._update_column_family_test_helper(gc_rule=gc_rule)


class _Cluster(object):

    def __init__(self, name):
        self.name = name
