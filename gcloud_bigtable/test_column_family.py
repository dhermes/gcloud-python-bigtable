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
COLUMN_FAMILY_ID = 'column-family-id'


class TestGarbageCollectionRule(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule
        return GarbageCollectionRule

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        gc_rule = self._makeOne()
        self.assertEqual(gc_rule.max_num_versions, None)
        self.assertEqual(gc_rule.max_age, None)

    def test_constructor_failure(self):
        with self.assertRaises(TypeError):
            self._makeOne(max_num_versions=1, max_age=object())

    def test___eq__max_age(self):
        max_age = object()
        gc_rule1 = self._makeOne(max_age=max_age)
        gc_rule2 = self._makeOne(max_age=max_age)
        self.assertEqual(gc_rule1, gc_rule2)

    def test___eq__max_num_versions(self):
        gc_rule1 = self._makeOne(max_num_versions=2)
        gc_rule2 = self._makeOne(max_num_versions=2)
        self.assertEqual(gc_rule1, gc_rule2)

    def test___eq__type_differ(self):
        gc_rule1 = self._makeOne()
        gc_rule2 = object()
        self.assertNotEqual(gc_rule1, gc_rule2)

    def test___ne__same_value(self):
        gc_rule1 = self._makeOne()
        gc_rule2 = self._makeOne()
        comparison_val = (gc_rule1 != gc_rule2)
        self.assertFalse(comparison_val)

    def test_to_pb_too_many_values(self):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        # Fool the constructor by passing no values.
        gc_rule = self._makeOne()
        gc_rule.max_num_versions = object()
        gc_rule.max_age = object()
        with self.assertRaises(TypeError):
            gc_rule.to_pb()

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
        from gcloud_bigtable.column_family import GarbageCollectionRuleUnion
        return GarbageCollectionRuleUnion

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        rules = object()
        rule_union = self._makeOne(rules=rules)
        self.assertTrue(rule_union.rules is rules)

    def test___eq__(self):
        rules = object()
        gc_rule1 = self._makeOne(rules=rules)
        gc_rule2 = self._makeOne(rules=rules)
        self.assertEqual(gc_rule1, gc_rule2)

    def test___eq__type_differ(self):
        gc_rule1 = self._makeOne()
        gc_rule2 = object()
        self.assertNotEqual(gc_rule1, gc_rule2)

    def test___ne__same_value(self):
        gc_rule1 = self._makeOne()
        gc_rule2 = self._makeOne()
        comparison_val = (gc_rule1 != gc_rule2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2
        from gcloud_bigtable.column_family import GarbageCollectionRule

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
        from gcloud_bigtable.column_family import GarbageCollectionRule

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
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)
        return GarbageCollectionRuleIntersection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        rules = object()
        rule_intersection = self._makeOne(rules=rules)
        self.assertTrue(rule_intersection.rules is rules)

    def test___eq__(self):
        rules = object()
        gc_rule1 = self._makeOne(rules=rules)
        gc_rule2 = self._makeOne(rules=rules)
        self.assertEqual(gc_rule1, gc_rule2)

    def test___eq__type_differ(self):
        gc_rule1 = self._makeOne()
        gc_rule2 = object()
        self.assertNotEqual(gc_rule1, gc_rule2)

    def test___ne__same_value(self):
        gc_rule1 = self._makeOne()
        gc_rule2 = self._makeOne()
        comparison_val = (gc_rule1 != gc_rule2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        import datetime
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2
        from gcloud_bigtable.column_family import GarbageCollectionRule

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
        from gcloud_bigtable.column_family import GarbageCollectionRule

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


class TestColumnFamily(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import client
        from gcloud_bigtable import column_family as MUT
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
        from gcloud_bigtable.column_family import ColumnFamily
        return ColumnFamily

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        table = object()
        gc_rule = object()
        column_family = self._makeOne(COLUMN_FAMILY_ID, table, gc_rule=gc_rule)
        self.assertEqual(column_family.column_family_id, COLUMN_FAMILY_ID)
        self.assertTrue(column_family._table is table)
        self.assertTrue(column_family.gc_rule is gc_rule)

    def test_table_getter(self):
        table = object()
        column_family = self._makeOne(COLUMN_FAMILY_ID, table)
        self.assertTrue(column_family.table is table)

    def test_client_getter(self):
        client = object()
        table = _Table(None, client=client)
        column_family = self._makeOne(COLUMN_FAMILY_ID, table)
        self.assertTrue(column_family.client is client)

    def test_timeout_seconds_getter(self):
        timeout_seconds = 889
        table = _Table(None, timeout_seconds=timeout_seconds)
        column_family = self._makeOne(COLUMN_FAMILY_ID, table)
        self.assertEqual(column_family.timeout_seconds, timeout_seconds)

    def test_name_property(self):
        table_name = 'table_name'
        table = _Table(table_name)
        column_family = self._makeOne(COLUMN_FAMILY_ID, table)
        expected_name = table_name + '/columnFamilies/' + COLUMN_FAMILY_ID
        self.assertEqual(column_family.name, expected_name)

    def test___eq__(self):
        column_family_id = 'column_family_id'
        table = object()
        column_family1 = self._makeOne(column_family_id, table)
        column_family2 = self._makeOne(column_family_id, table)
        self.assertEqual(column_family1, column_family2)

    def test___eq__type_differ(self):
        column_family1 = self._makeOne('column_family_id', None)
        column_family2 = object()
        self.assertNotEqual(column_family1, column_family2)

    def test___ne__same_value(self):
        column_family_id = 'column_family_id'
        table = object()
        column_family1 = self._makeOne(column_family_id, table)
        column_family2 = self._makeOne(column_family_id, table)
        comparison_val = (column_family1 != column_family2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        column_family1 = self._makeOne('column_family_id1', 'table1')
        column_family2 = self._makeOne('column_family_id2', 'table2')
        self.assertNotEqual(column_family1, column_family2)

    def _create_test_helper(self, gc_rule=None):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)

        # Create request_pb
        table_name = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
                      '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
        if gc_rule is None:
            column_family = data_pb2.ColumnFamily()
        else:
            column_family = data_pb2.ColumnFamily(gc_rule=gc_rule.to_pb())
        request_pb = messages_pb2.CreateColumnFamilyRequest(
            name=table_name,
            column_family_id=COLUMN_FAMILY_ID,
            column_family=column_family,
        )

        # Create response_pb
        response_pb = data_pb2.ColumnFamily()

        # Create expected_result.
        expected_result = None  # create() has no return value.

        # We must create the column family from the client.
        TEST_CASE = self
        timeout_seconds = 4

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = cluster.table(TABLE_ID)
            column_family = TEST_CASE._makeOne(COLUMN_FAMILY_ID, table,
                                               gc_rule=gc_rule)
            return column_family.create(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('CreateColumnFamily', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)

    def test_create(self):
        self._create_test_helper(gc_rule=None)

    def test_create_with_gc_rule(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule
        gc_rule = GarbageCollectionRule(max_num_versions=1337)
        self._create_test_helper(gc_rule=gc_rule)

    def _update_test_helper(self, gc_rule=None):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)

        # Create request_pb
        column_family_name = (
            'projects/' + PROJECT_ID + '/zones/' + ZONE +
            '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID +
            '/columnFamilies/' + COLUMN_FAMILY_ID)
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
        expected_result = None  # update() has no return value.

        # We must create the column family from the client.
        TEST_CASE = self
        timeout_seconds = 28

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = cluster.table(TABLE_ID)
            column_family = TEST_CASE._makeOne(COLUMN_FAMILY_ID, table,
                                               gc_rule=gc_rule)
            return column_family.update(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('UpdateColumnFamily', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)

    def test_update(self):
        self._update_test_helper(gc_rule=None)

    def test_update_with_gc_rule(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule
        gc_rule = GarbageCollectionRule(max_num_versions=1337)
        self._update_test_helper(gc_rule=gc_rule)

    def test_delete(self):
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2

        # Create request_pb
        column_family_name = (
            'projects/' + PROJECT_ID + '/zones/' + ZONE +
            '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID +
            '/columnFamilies/' + COLUMN_FAMILY_ID)
        request_pb = messages_pb2.DeleteColumnFamilyRequest(
            name=column_family_name)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # We must create the cluster with the client passed in
        # and then the table with that cluster.
        TEST_CASE = self
        timeout_seconds = 7

        def result_method(client):
            cluster = client.cluster(ZONE, CLUSTER_ID)
            table = cluster.table(TABLE_ID)
            column_family = TEST_CASE._makeOne(COLUMN_FAMILY_ID, table)
            return column_family.delete(timeout_seconds=timeout_seconds)

        self._grpc_client_test_helper('DeleteColumnFamily', result_method,
                                      request_pb, response_pb, expected_result,
                                      PROJECT_ID,
                                      timeout_seconds=timeout_seconds)


class Test__gc_rule_from_pb(unittest2.TestCase):

    def _callFUT(self, gc_rule_pb):
        from gcloud_bigtable.column_family import _gc_rule_from_pb
        return _gc_rule_from_pb(gc_rule_pb)

    def test_empty(self):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)

        gc_rule_pb = data_pb2.GcRule()
        self.assertEqual(self._callFUT(gc_rule_pb), None)

    def test_failure(self):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import duration_pb2

        gc_rule_pb1 = data_pb2.GcRule(max_num_versions=1)
        gc_rule_pb2 = data_pb2.GcRule(
            max_age=duration_pb2.Duration(seconds=1),
        )
        # Since a oneof field, google.protobuf doesn't allow both
        # to be set, so we fake it.
        gc_rule_pb3 = data_pb2.GcRule()
        gc_rule_pb3._fields.update(gc_rule_pb1._fields)
        gc_rule_pb3._fields.update(gc_rule_pb2._fields)

        with self.assertRaises(ValueError):
            self._callFUT(gc_rule_pb3)

    def test_max_num_versions(self):
        from gcloud_bigtable.column_family import GarbageCollectionRule

        orig_rule = GarbageCollectionRule(max_num_versions=1)
        gc_rule_pb = orig_rule.to_pb()
        result = self._callFUT(gc_rule_pb)
        self.assertTrue(isinstance(result, GarbageCollectionRule))
        self.assertEqual(result, orig_rule)

    def test_max_age(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule

        orig_rule = GarbageCollectionRule(
            max_age=datetime.timedelta(seconds=1))
        gc_rule_pb = orig_rule.to_pb()
        result = self._callFUT(gc_rule_pb)
        self.assertTrue(isinstance(result, GarbageCollectionRule))
        self.assertEqual(result, orig_rule)

    def test_union(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule
        from gcloud_bigtable.column_family import GarbageCollectionRuleUnion

        rule1 = GarbageCollectionRule(max_num_versions=1)
        rule2 = GarbageCollectionRule(
            max_age=datetime.timedelta(seconds=1))
        orig_rule = GarbageCollectionRuleUnion(rules=[rule1, rule2])
        gc_rule_pb = orig_rule.to_pb()
        result = self._callFUT(gc_rule_pb)
        self.assertTrue(isinstance(result, GarbageCollectionRuleUnion))
        self.assertEqual(result, orig_rule)

    def test_intersection(self):
        import datetime
        from gcloud_bigtable.column_family import GarbageCollectionRule
        from gcloud_bigtable.column_family import (
            GarbageCollectionRuleIntersection)

        rule1 = GarbageCollectionRule(max_num_versions=1)
        rule2 = GarbageCollectionRule(
            max_age=datetime.timedelta(seconds=1))
        orig_rule = GarbageCollectionRuleIntersection(rules=[rule1, rule2])
        gc_rule_pb = orig_rule.to_pb()
        result = self._callFUT(gc_rule_pb)
        self.assertTrue(isinstance(result, GarbageCollectionRuleIntersection))
        self.assertEqual(result, orig_rule)

    def test_unknown_field_name(self):
        from google.protobuf.descriptor import FieldDescriptor
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as data_pb2)

        gc_rule_pb = data_pb2.GcRule()
        fake_descriptor_name = 'not-union'
        descriptor_args = (fake_descriptor_name,) + (None,) * 12
        fake_descriptor = FieldDescriptor(*descriptor_args)
        gc_rule_pb._fields[fake_descriptor] = None
        self.assertEqual(self._callFUT(gc_rule_pb), None)


class _Table(object):

    def __init__(self, name, client=None, timeout_seconds=None):
        self.name = name
        self.client = client
        self.timeout_seconds = timeout_seconds
