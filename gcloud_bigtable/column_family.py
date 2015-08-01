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
from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._helpers import _duration_pb_to_timedelta
from gcloud_bigtable._helpers import _timedelta_to_duration_pb
from gcloud_bigtable._helpers import make_stub
from gcloud_bigtable.constants import TABLE_ADMIN_HOST
from gcloud_bigtable.constants import TABLE_ADMIN_PORT
from gcloud_bigtable.constants import TABLE_STUB_FACTORY


class GarbageCollectionRule(object):
    """Table garbage collection rule.

    Cells in the table fitting the rule will be deleted during
    garbage collection.

    These values can be combined via :class:`GarbageCollectionRuleUnion` and
    :class:`GarbageCollectionRuleIntersection`.

    .. note::

        At most one of ``max_num_versions`` and ``max_age`` can be specified
        at once.

    .. note::

        A string ``gc_expression`` can also be used with API requests, but
        that value would be superceded by a ``gc_rule``. As a result, we
        don't support that feature and instead support via this native
        object.

    :type max_num_versions: int
    :param max_num_versions: The maximum number of versions

    :type max_age: :class:`datetime.timedelta`
    :param max_age: The maximum age allowed for a cell in the table.

    :raises: :class:`TypeError <exceptions.TypeError>` if both
             ``max_num_versions`` and ``max_age`` are set.
    """

    def __init__(self, max_num_versions=None, max_age=None):
        self.max_num_versions = max_num_versions
        self.max_age = max_age
        self._check_single_value()

    def _check_single_value(self):
        """Checks that at most one value is set on the instance.

        :raises: :class:`TypeError <exceptions.TypeError>` if not exactly one
                 value set on the instance.
        """
        if self.max_num_versions is not None and self.max_age is not None:
            raise TypeError('At most one of max_num_versions and '
                            'max_age can be set')

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (other.max_num_versions == self.max_num_versions and
                other.max_age == self.max_age)

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_pb(self):
        """Converts the :class:`GarbageCollectionRule` to a protobuf.

        :rtype: :class:`data_pb2.GcRule`
        :returns: The converted current object.
        """
        self._check_single_value()
        gc_rule_kwargs = {}
        if self.max_num_versions is not None:
            gc_rule_kwargs['max_num_versions'] = self.max_num_versions
        if self.max_age is not None:
            gc_rule_kwargs['max_age'] = _timedelta_to_duration_pb(self.max_age)
        return data_pb2.GcRule(**gc_rule_kwargs)


class GarbageCollectionRuleUnion(object):
    """Union of garbage collection rules.

    :type rules: list
    :param rules: List of :class:`GarbageCollectionRule`,
                  :class:`GarbageCollectionRuleUnion` and/or
                  :class:`GarbageCollectionRuleIntersection`
    """

    def __init__(self, rules=None):
        self.rules = rules

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return other.rules == self.rules

    def __ne__(self, other):
        return not self.__eq__(other)

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
    :param rules: List of :class:`GarbageCollectionRule`,
                  :class:`GarbageCollectionRuleUnion` and/or
                  :class:`GarbageCollectionRuleIntersection`
    """

    def __init__(self, rules=None):
        self.rules = rules

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return other.rules == self.rules

    def __ne__(self, other):
        return not self.__eq__(other)

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

    We can use a :class:`ColumnFamily` to:

    * :meth:`create` itself
    * :meth:`update` itself
    * :meth:`delete` itself

    :type column_family_id: string
    :param column_family_id: The ID of the column family.

    :type table: :class:`.table.Table`
    :param table: The table that owns the column family.

    :type gc_rule: :class:`GarbageCollectionRule`,
                   :class:`GarbageCollectionRuleUnion` or
                   :class:`GarbageCollectionRuleIntersection`
    :param gc_rule: (Optional) The garbage collection settings for this
                    column family.
    """

    def __init__(self, column_family_id, table, gc_rule=None):
        self.column_family_id = column_family_id
        self._table = table
        self.gc_rule = gc_rule

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

        :rtype: int
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

            ``"projects/../zones/../clusters/../tables/../columnFamilies/.."``

        :rtype: string
        :returns: The column family name.
        """
        return self.table.name + '/columnFamilies/' + self.column_family_id

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (other.column_family_id == self.column_family_id and
                other.gc_rule == self.gc_rule and
                other.table == self.table)

    def __ne__(self, other):
        return not self.__eq__(other)

    def create(self, timeout_seconds=None):
        """Create this column family.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                column family.
        """
        if self.gc_rule is None:
            column_family = data_pb2.ColumnFamily()
        else:
            column_family = data_pb2.ColumnFamily(gc_rule=self.gc_rule.to_pb())
        request_pb = messages_pb2.CreateColumnFamilyRequest(
            name=self.table.name,
            column_family_id=self.column_family_id,
            column_family=column_family,
        )

        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.CreateColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `data_pb2.ColumnFamily`
            response.result()

    def update(self, timeout_seconds=None):
        """Update this column family.

        .. note::

            The Bigtable Table Admin API currently returns

             ``BigtableTableService.UpdateColumnFamily is not yet implemented``

            when this method is used. It's unclear when this method will
            actually be supported by the API.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                column family.
        """
        request_kwargs = {'name': self.name}
        if self.gc_rule is not None:
            request_kwargs['gc_rule'] = self.gc_rule.to_pb()
        request_pb = data_pb2.ColumnFamily(**request_kwargs)

        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.UpdateColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `data_pb2.ColumnFamily`
            response.result()

    def delete(self, timeout_seconds=None):
        """Delete this column family.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                column family.
        """
        request_pb = messages_pb2.DeleteColumnFamilyRequest(name=self.name)
        stub = make_stub(self.client, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.DeleteColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()


def _gc_rule_from_pb(gc_rule_pb):
    """Convert a protobuf GC rule to a Python version.

    :type gc_rule_pb: :class:`data_pb2.GcRule`
    :param gc_rule_pb: The GC rule to convert.

    :rtype: :class:`GarbageCollectionRule`,
            :class:`GarbageCollectionRuleUnion`,
            :class:`GarbageCollectionRuleIntersection` or
            :class:`NoneType <types.NoneType>`
    :returns: An instance of one of the native rules defined
              in :module:`column_family` or :data:`None` if no values were
              set on the protobuf passed in.
    :raises: :class:`ValueError` if more than one property has been set on
             the GC rule.
    """
    all_fields = [field.name for field in gc_rule_pb._fields]
    if len(all_fields) == 0:
        return None
    elif len(all_fields) > 1:
        raise ValueError('At most one field can be set on a GC rule.')

    field_name = all_fields[0]
    if field_name == 'max_num_versions':
        return GarbageCollectionRule(
            max_num_versions=gc_rule_pb.max_num_versions)
    elif field_name == 'max_age':
        max_age = _duration_pb_to_timedelta(gc_rule_pb.max_age)
        return GarbageCollectionRule(max_age=max_age)
    elif field_name == 'union':
        all_rules = gc_rule_pb.union.rules
        return GarbageCollectionRuleUnion(
            rules=[_gc_rule_from_pb(rule) for rule in all_rules])
    elif field_name == 'intersection':
        all_rules = gc_rule_pb.intersection.rules
        return GarbageCollectionRuleIntersection(
            rules=[_gc_rule_from_pb(rule) for rule in all_rules])
