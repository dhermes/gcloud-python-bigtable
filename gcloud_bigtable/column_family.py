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

"""User friendly container for Google Cloud Bigtable Column Family."""


from gcloud_bigtable._generated import bigtable_table_data_pb2 as data_pb2
from gcloud_bigtable._helpers import _timedelta_to_duration_pb


class GarbageCollectionRule(object):
    """Table garbage collection rule.

    Cells in the table fitting the rule will be deleted during
    garbage collection.

    These values can be combined via :class:`GarbageCollectionRuleUnion` and
    :class:`GarbageCollectionRuleIntersection.`

    .. note::

        At most one of ``max_num_versions`` and ``max_age`` can be specified
        at once.

    .. note::

        A string ``gc_expression`` can also be used with API requests, but
        that value would be superceded by a ``gc_rule``. As a result, we
        don't support that feature and instead support via this native
        object.

    :type max_num_versions: integer
    :param max_num_versions: The maximum number of versions

    :type max_age: :class:`datetime.timedelta`
    :param max_age: The maximum age allowed for a cell in the table.

    :raises: :class:`ValueError` if both ``max_num_versions`` and ``max_age``
             are set.
    """

    def __init__(self, max_num_versions=None, max_age=None):
        if max_num_versions is not None and max_age is not None:
            raise ValueError('At most one of max_num_versions and '
                             'max_age can be set')
        self.max_num_versions = max_num_versions
        self.max_age = max_age

    def to_pb(self):
        """Converts the :class:`GarbageCollectionRule` to a protobuf.

        :rtype: :class:`data_pb2.GcRule`
        :returns: The converted current object.
        """
        gc_rule_kwargs = {}
        if self.max_num_versions is not None:
            gc_rule_kwargs['max_num_versions'] = self.max_num_versions
        if self.max_age is not None:
            gc_rule_kwargs['max_age'] = _timedelta_to_duration_pb(self.max_age)
        return data_pb2.GcRule(**gc_rule_kwargs)


class GarbageCollectionRuleUnion(object):
    """Union of garbage collection rules.

    :type rules: list
    :param rules: List of garbage collection rules, unions and/or
                  intersections.
    """

    def __init__(self, rules=None):
        self.rules = rules

    def to_pb(self):
        """Converts the union into a single gc rule as a protobuf.

        :rtype: :class:`data_pb2.GcRule`
        :returns: The converted current object.
        """
        union = data_pb2.GcRule.Union(
            rules=[rule.to_pb() for rule in self.rules])
        return data_pb2.GcRule(union=union)


class GarbageCollectionRuleIntersection(object):
    """Intersection of garbage collection rules.

    :type rules: list
    :param rules: List of garbage collection rules, unions and/or
                  intersections.
    """

    def __init__(self, rules=None):
        self.rules = rules

    def to_pb(self):
        """Converts the intersection into a single gc rule as a protobuf.

        :rtype: :class:`data_pb2.GcRule`
        :returns: The converted current object.
        """
        intersection = data_pb2.GcRule.Intersection(
            rules=[rule.to_pb() for rule in self.rules])
        return data_pb2.GcRule(intersection=intersection)


class ColumnFamily(object):
    """Representation of a Google Cloud Bigtable Column Family.

    :type column_family_id: string
    :param column_family_id: The ID of the column family.

    :type table: :class:`.table.Table`
    :param table: The table that owns the column family.
    """

    def __init__(self, column_family_id, table):
        self.column_family_id = column_family_id
        self._table = table

    @property
    def table(self):
        """Getter for column family's table.

        :rtype: :class:`.table.Table`
        :returns: The table stored on the column family.
        """
        return self._table

    @property
    def client(self):
        """Getter for column family's client.

        :rtype: :class:`.client.Client`
        :returns: The client that owns this column family.
        """
        return self.table.client

    @property
    def timeout_seconds(self):
        """Getter for column family's default timeout seconds.

        :rtype: integer
        :returns: The timeout seconds default.
        """
        return self.table.timeout_seconds

    @property
    def name(self):
        """Column family name used in requests.

        .. note::

          This property will not change if ``column_family_id`` does not, but
          the return value is not cached.

        The table name is of the form
        "projects/../zones/../clusters/../tables/../columnFamilies/.."

        :rtype: string
        :returns: The column family name.
        """
        return self.table.name + '/columnFamilies/' + self.column_family_id
