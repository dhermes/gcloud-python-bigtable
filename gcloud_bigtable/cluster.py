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

"""User friendly container for Google Cloud Bigtable Cluster."""


import re

from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as table_messages_pb2)
from gcloud_bigtable._generated import operations_pb2
from gcloud_bigtable._helpers import _parse_pb_any_to_native
from gcloud_bigtable._helpers import _pb_timestamp_to_datetime
from gcloud_bigtable._helpers import _require_pb_property
from gcloud_bigtable.table import Table


_CLUSTER_NAME_RE = re.compile(r'^projects/(?P<project_id>[^/]+)/'
                              r'zones/(?P<zone>[^/]+)/clusters/'
                              r'(?P<cluster_id>[a-z][-a-z0-9]*)$')
_OPERATION_NAME_RE = re.compile(r'^operations/projects/([^/]+)/zones/([^/]+)/'
                                r'clusters/([a-z][-a-z0-9]*)/operations/'
                                r'(?P<operation_id>\d+)$')


def _prepare_create_request(cluster):
    """Creates a protobuf request for a CreateCluster request.

    :type cluster: :class:`Cluster`
    :param cluster: The cluster to be created.

    :rtype: :class:`.messages_pb2.CreateClusterRequest`
    :returns: The CreateCluster request object containing the cluster info.
    """
    zone_full_name = ('projects/' + cluster.project_id +
                      '/zones/' + cluster.zone)
    return messages_pb2.CreateClusterRequest(
        name=zone_full_name,
        cluster_id=cluster.cluster_id,
        cluster=data_pb2.Cluster(
            display_name=cluster.display_name,
            serve_nodes=cluster.serve_nodes,
        ),
    )


def _process_operation(operation_pb):
    """Processes a create protobuf response.

    :type operation_pb: :class:`operations_pb2.Operation`
    :param operation_pb: The long-running operation response from a
                         Create/Update/Undelete cluster request.

    :rtype: tuple
    :returns: A pair of an integer and datetime stamp. The integer is the ID
              of the operation (``operation_id``) and the timestamp when
              the create operation began (``operation_begin``).
    :raises: :class:`ValueError <exceptions.ValueError>` if the operation name
             doesn't match the :data:`_OPERATION_NAME_RE` regex.
    """
    match = _OPERATION_NAME_RE.match(operation_pb.name)
    if match is None:
        raise ValueError('Cluster create operation name was not in the '
                         'expected format.', operation_pb.name)
    operation_id = int(match.group('operation_id'))

    request_metadata = _parse_pb_any_to_native(operation_pb.metadata)
    operation_begin = _pb_timestamp_to_datetime(
        request_metadata.request_time)

    return operation_id, operation_begin


class Cluster(object):
    """Representation of a Google Cloud Bigtable Cluster.

    We can use a :class:`Cluster` to:

    * :meth:`reload` itself
    * :meth:`create` itself
    * Check if an :meth:`operation_finished` (each of :meth:`create`,
      :meth:`update` and :meth:`undelete` return with long-running operations)
    * :meth:`update` itself
    * :meth:`delete` itself
    * :meth:`undelete` itself

    .. note::

        For now, we leave out the properties ``hdd_bytes`` and ``ssd_bytes``
        (both integers) and also the ``default_storage_type`` (an enum)
        which if not sent will end up as :data:`.data_pb2.STORAGE_SSD`.

    :type zone: str
    :param zone: The name of the zone where the cluster resides.

    :type cluster_id: str
    :param cluster_id: The ID of the cluster.

    :type client: :class:`.client.Client`
    :param client: The client that owns the cluster. Provides
                   authorization and a project ID.

    :type display_name: str
    :param display_name: (Optional) The display name for the cluster in the
                         Cloud Console UI. (Must be between 4 and 30
                         characters.) If this value is not set in the
                         constructor, will fall back to the cluster ID.

    :type serve_nodes: int
    :param serve_nodes: (Optional) The number of nodes in the cluster.
                        Defaults to 3.
    """

    def __init__(self, zone, cluster_id, client,
                 display_name=None, serve_nodes=3):
        self.zone = zone
        self.cluster_id = cluster_id
        self.display_name = display_name or cluster_id
        self.serve_nodes = serve_nodes
        self._client = client
        self._operation_type = None
        self._operation_id = None
        self._operation_begin = None

    def _update_from_pb(self, cluster_pb):
        self.display_name = _require_pb_property(
            cluster_pb, 'display_name', None)
        self.serve_nodes = _require_pb_property(
            cluster_pb, 'serve_nodes', None)

    @classmethod
    def from_pb(cls, cluster_pb, client):
        """Creates a cluster instance from a protobuf.

        :type cluster_pb: :class:`bigtable_cluster_data_pb2.Cluster`
        :param cluster_pb: A cluster protobuf object.

        :type client: :class:`.client.Client`
        :param client: The client that owns the cluster.

        :rtype: :class:`Cluster`
        :returns: The cluster parsed from the protobuf response.
        :raises: :class:`ValueError <exceptions.ValueError>` if the cluster
                 name does not match :data:`_CLUSTER_NAME_RE` or if the parsed
                 project ID does not match the project ID on the client.
        """
        match = _CLUSTER_NAME_RE.match(cluster_pb.name)
        if match is None:
            raise ValueError('Cluster protobuf name was not in the '
                             'expected format.', cluster_pb.name)
        if match.group('project_id') != client.project_id:
            raise ValueError('Project ID on cluster does not match the '
                             'project ID on the client')

        result = cls(match.group('zone'), match.group('cluster_id'), client)
        result._update_from_pb(cluster_pb)
        return result

    @property
    def client(self):
        """Getter for cluster's client.

        :rtype: :class:`.client.Client`
        :returns: The client stored on the cluster.
        """
        return self._client

    @property
    def project_id(self):
        """Getter for cluster's project ID.

        :rtype: str
        :returns: The project ID for the cluster (is stored on the client).
        """
        return self._client.project_id

    @property
    def timeout_seconds(self):
        """Getter for cluster's default timeout seconds.

        :rtype: int
        :returns: The timeout seconds default stored on the cluster's client.
        """
        return self._client.timeout_seconds

    @property
    def name(self):
        """Cluster name used in requests.

        .. note::
          This property will not change if ``zone`` and ``cluster_id`` do not,
          but the return value is not cached.

        The cluster name is of the form

            ``"projects/{project_id}/zones/{zone}/clusters/{cluster_id}"``

        :rtype: str
        :returns: The cluster name.
        """
        return (self.client.project_name + '/zones/' + self.zone +
                '/clusters/' + self.cluster_id)

    def table(self, table_id):
        """Factory to create a table associated with this cluster.

        :type table_id: str
        :param table_id: The ID of the table.

        :rtype: :class:`Table <gcloud_bigtable.table.Table>`
        :returns: The table owned by this cluster.
        """
        return Table(table_id, self)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        # NOTE: This does not compare the configuration values, such as
        #       the serve_nodes or display_name. This is intentional, since
        #       the same cluster can be in different states if not
        #       synchronized. This suggests we should use `project_id`
        #       instead of `client` for the third comparison.
        return (other.zone == self.zone and
                other.cluster_id == self.cluster_id and
                other.client == self.client)

    def __ne__(self, other):
        return not self.__eq__(other)

    def reload(self, timeout_seconds=None):
        """Reload the metadata for this cluster.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.
        """
        request_pb = messages_pb2.GetClusterRequest(name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.cluster_stub.GetCluster.async(request_pb,
                                                             timeout_seconds)
        # We expect a `._generated.bigtable_cluster_data_pb2.Cluster`.
        cluster_pb = response.result()

        # NOTE: _update_from_pb does not check that the project, zone and
        #       cluster ID on the response match the request.
        self._update_from_pb(cluster_pb)

    def operation_finished(self, timeout_seconds=None):
        """Check if the current operation has finished.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.

        :rtype: bool
        :returns: A boolean indicating if the current operation has completed.
        :raises: :class:`ValueError <exceptions.ValueError>` if there is no
                 current operation set.
        """
        if self._operation_id is None:
            raise ValueError('There is no current operation.')

        operation_name = ('operations/' + self.name +
                          '/operations/%d' % (self._operation_id,))
        request_pb = operations_pb2.GetOperationRequest(name=operation_name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.operations_stub.GetOperation.async(
            request_pb, timeout_seconds)
        # We expact a `._generated.operations_pb2.Operation`.
        operation_pb = response.result()

        if operation_pb.done:
            self._operation_type = None
            self._operation_id = None
            self._operation_begin = None
            return True
        else:
            return False

    def create(self, timeout_seconds=None):
        """Create this cluster.

        .. note::

            Uses the ``project_id``, ``zone`` and ``cluster_id`` on the current
            :class:`Cluster` in addition to the ``display_name`` and
            ``serve_nodes``. If you'd like to change them before creating,
            reset the values via

            .. code:: python

                cluster.display_name = 'New display name'
                cluster.cluster_id = 'i-changed-my-mind'

            before calling :meth:`create`.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.
        """
        request_pb = _prepare_create_request(self)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.cluster_stub.CreateCluster.async(
            request_pb, timeout_seconds)
        # We expect an `operations_pb2.Operation`.
        cluster_pb = response.result()

        self._operation_type = 'create'
        self._operation_id, self._operation_begin = _process_operation(
            cluster_pb.current_operation)

    def update(self, timeout_seconds=None):
        """Update this cluster.

        .. note::

            Updates the ``display_name`` and ``serve_nodes``. If you'd like to
            change them before updating, reset the values via

            .. code:: python

                cluster.display_name = 'New display name'
                cluster.serve_nodes = 3

            before calling :meth:`update`.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.
        """
        request_pb = data_pb2.Cluster(
            name=self.name,
            display_name=self.display_name,
            serve_nodes=self.serve_nodes,
        )
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.cluster_stub.UpdateCluster.async(
            request_pb, timeout_seconds)
        # We expect a `._generated.bigtable_cluster_data_pb2.Cluster`.
        cluster_pb = response.result()

        self._operation_type = 'update'
        self._operation_id, self._operation_begin = _process_operation(
            cluster_pb.current_operation)

    def delete(self, timeout_seconds=None):
        """Delete this cluster.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.
        """
        request_pb = messages_pb2.DeleteClusterRequest(name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.cluster_stub.DeleteCluster.async(
            request_pb, timeout_seconds)
        # We expect a `._generated.empty_pb2.Empty`
        response.result()

    def undelete(self, timeout_seconds=None):
        """Undelete this cluster.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.
        """
        request_pb = messages_pb2.UndeleteClusterRequest(name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.cluster_stub.UndeleteCluster.async(
            request_pb, timeout_seconds)
        # We expect a `._generated.operations_pb2.Operation`
        operation_pb2 = response.result()

        self._operation_type = 'undelete'
        self._operation_id, self._operation_begin = _process_operation(
            operation_pb2)

    def list_tables(self, timeout_seconds=None):
        """List the tables in this cluster.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on
                                cluster.

        :rtype: list of :class:`Table <gcloud_bigtable.table.Table>`
        :returns: The list of tables owned by the cluster.
        :raises: :class:`ValueError <exceptions.ValueError>` if one of the
                 returned tables has a name that is not of the expected format.
        """
        request_pb = table_messages_pb2.ListTablesRequest(name=self.name)
        timeout_seconds = timeout_seconds or self.timeout_seconds
        response = self.client.table_stub.ListTables.async(request_pb,
                                                           timeout_seconds)
        # We expect a `table_messages_pb2.ListTablesResponse`
        table_list_pb = response.result()

        result = []
        for table_pb in table_list_pb.tables:
            before, table_id = table_pb.name.split(
                self.name + '/tables/', 1)
            if before != '':
                raise ValueError('Table name %s not of expected format' % (
                    table_pb.name,))
            result.append(self.table(table_id))

        return result
