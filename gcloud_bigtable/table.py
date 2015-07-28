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

"""User friendly container for Google Cloud Bigtable Table."""


from gcloud_bigtable._generated import bigtable_table_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._generated import bigtable_table_service_pb2
from gcloud_bigtable._helpers import TIMEOUT_SECONDS
from gcloud_bigtable._helpers import _timedelta_to_duration_pb
from gcloud_bigtable._helpers import make_stub


TABLE_STUB_FACTORY = (bigtable_table_service_pb2.
                      early_adopter_create_BigtableTableService_stub)
TABLE_ADMIN_HOST = 'bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""
TABLE_ADMIN_PORT = 443
"""Table Admin API request port."""


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


class Table(object):
    """Representation of a Google Cloud Bigtable Table.

    .. note::

        We don't define any properties on a table other than the name. As
        the proto says, in a request:

          "The `name` field of the Table and all of its ColumnFamilies must
           be left blank, and will be populated in the response."

        This leaves only the ``current_operation`` and ``granularity``
        fields. The ``current_operation`` is only used for responses while
        ``granularity`` is an enum with only one value.

    We can use a :class:`Table` to:

    * Check if it :meth:`exists`
    * :meth:`create` itself
    * :meth:`rename` itself
    * :meth:`delete` itself

    :type table_id: string
    :param table_id: The ID of the table.

    :type cluster: :class:`.cluster.Cluster`
    :param cluster: The cluster that owns the table.
    """

    def __init__(self, table_id, cluster):
        self.table_id = table_id
        self._cluster = cluster

    @property
    def cluster(self):
        """Getter for table's cluster.

        :rtype: :class:`.cluster.Cluster`
        :returns: The cluster stored on the table.
        """
        return self._cluster

    @property
    def credentials(self):
        """Getter for table's credentials.

        :rtype: :class:`oauth2client.client.OAuth2Credentials`
        :returns: The credentials stored on the table's client.
        """
        return self._cluster.credentials

    @property
    def name(self):
        """Table name used in requests.

        .. note::

          This property will not change if ``table_id`` does not, but the
          return value is not cached.

        The table name is of the form
        "projects/../zones/../clusters/../tables/{table_id}"

        :rtype: string
        :returns: The table name.
        """
        return self.cluster.name + '/tables/' + self.table_id

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (other.table_id == self.table_id and
                other.cluster == self.cluster)

    def __ne__(self, other):
        return not self.__eq__(other)

    def exists(self, timeout_seconds=TIMEOUT_SECONDS):
        """Check if this table exists.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: boolean
        :returns: Boolean indicating if this table exists. If it does not
                  exist, an exception will be thrown by the API call.
        """
        request_pb = messages_pb2.GetTableRequest(name=self.name)
        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.GetTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.Table`
            response.result()

        return True

    def create(self, initial_split_keys=None, timeout_seconds=TIMEOUT_SECONDS):
        """Creates this table.

        .. note::

            Though a :class:`._generated.bigtable_table_data_pb2.Table` is also
            allowed (as the ``table`` property) in a create table request, we
            do not support it in this method. As mentioned in the
            :class:`Table` docstring, the name is the only useful property in
            the table proto.

        .. note::

            A create request returns a
            :class:`._generated.bigtable_table_data_pb2.Table` but we don't use
            this response. The proto definition allows for the inclusion of a
            ``current_operation`` in the response, but in example usage so far,
            it seems the Bigtable API does not return any operation.

        :type initial_split_keys: iterable of strings
        :param initial_split_keys: (Optional) List of row keys that will be
                                   used to initially split the table into
                                   several tablets (Tablets are similar to
                                   HBase regions). Given two split keys,
                                   ``"s1"`` and ``"s2"``, three tablets will be
                                   created, spanning the key ranges:
                                   [, s1), [s1, s2), [s2, ).

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        request_pb = messages_pb2.CreateTableRequest(
            initial_split_keys=initial_split_keys or [],
            name=self.cluster.name,
            table_id=self.table_id,
        )
        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.CreateTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.Table`
            response.result()

    def rename(self, new_table_id, timeout_seconds=TIMEOUT_SECONDS):
        """Rename this table.

        .. note::

            This cannot be used to move tables between clusters,
            zones, or projects.

        :type new_table_id: string
        :param new_table_id: The new name table ID.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        request_pb = messages_pb2.RenameTableRequest(
            name=self.name,
            new_id=new_table_id,
        )
        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.RenameTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()

    def delete(self, timeout_seconds=TIMEOUT_SECONDS):
        """Delete this table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        request_pb = messages_pb2.DeleteTableRequest(name=self.name)
        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.DeleteTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()

    def create_column_family(self, column_family_id, gc_rule=None,
                             timeout_seconds=TIMEOUT_SECONDS):
        """Create a column family in this table.

        :type column_family_id: string
        :param column_family_id: The ID of the column family.

        :type gc_rule: :class:`GarbageCollectionRule`,
                       :class:`GarbageCollectionRuleUnion` or
                       :class:`GarbageCollectionRuleIntersection`
        :param gc_rule: The garbage collection settings for the column family.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        if gc_rule is None:
            column_family = data_pb2.ColumnFamily()
        else:
            column_family = data_pb2.ColumnFamily(gc_rule=gc_rule.to_pb())
        request_pb = messages_pb2.CreateColumnFamilyRequest(
            name=self.name,
            column_family_id=column_family_id,
            column_family=column_family,
        )

        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.CreateColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.ColumnFamily`
            response.result()

    def update_column_family(self, column_family_id, gc_rule=None,
                             timeout_seconds=TIMEOUT_SECONDS):
        """Update a column family in this table.

        :type column_family_id: string
        :param column_family_id: The ID of the column family.

        :type gc_rule: :class:`GarbageCollectionRule`,
                       :class:`GarbageCollectionRuleUnion` or
                       :class:`GarbageCollectionRuleIntersection`
        :param gc_rule: The garbage collection settings for the column family.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        column_family_name = self.name + '/columnFamilies/' + column_family_id
        request_kwargs = {'name': column_family_name}
        if gc_rule is not None:
            request_kwargs['gc_rule'] = gc_rule.to_pb()
        request_pb = data_pb2.ColumnFamily(**request_kwargs)

        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.UpdateColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.ColumnFamily`
            response.result()

    def delete_column_family(self, column_family_id,
                             timeout_seconds=TIMEOUT_SECONDS):
        """Delete a column family in this table.

        :type column_family_id: string
        :param column_family_id: The ID of the column family.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        column_family_name = self.name + '/columnFamilies/' + column_family_id
        request_pb = messages_pb2.DeleteColumnFamilyRequest(
            name=column_family_name)
        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.DeleteColumnFamily.async(request_pb,
                                                     timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()
