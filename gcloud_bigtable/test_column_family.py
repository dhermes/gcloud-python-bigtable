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
        from gcloud_bigtable.column_family import GarbageCollectionRuleUnion
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


class TestColumnFamily(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.column_family import ColumnFamily
        return ColumnFamily

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        table = object()
        column_family = self._makeOne(COLUMN_FAMILY_ID, table)
        self.assertEqual(column_family.column_family_id, COLUMN_FAMILY_ID)
        self.assertTrue(column_family._table is table)

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


class _Table(object):

    def __init__(self, name, client=None, timeout_seconds=None):
        self.name = name
        self.client = client
        self.timeout_seconds = timeout_seconds
