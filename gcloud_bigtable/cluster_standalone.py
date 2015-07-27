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
from gcloud_bigtable._generated import bigtable_cluster_service_pb2
from gcloud_bigtable._helpers import _CLUSTER_CREATE_METADATA
from gcloud_bigtable._helpers import _parse_pb_any_to_native
from gcloud_bigtable._helpers import _pb_timestamp_to_datetime
from gcloud_bigtable._helpers import _require_pb_property
from gcloud_bigtable.connection import TIMEOUT_SECONDS
from gcloud_bigtable.connection import make_stub


_CLUSTER_NAME_RE = re.compile(r'^projects/(?P<project_id>[^/]+)/'
                              r'zones/(?P<zone>[^/]+)/clusters/'
                              r'(?P<cluster_id>[a-z][-a-z0-9]*)$')
_OPERATION_NAME_RE = re.compile(r'^operations/projects/([^/]+)/zones/([^/]+)/'
                                r'clusters/([a-z][-a-z0-9]*)/operations/'
                                r'(?P<operation_id>\d+)$')

CLUSTER_ADMIN_HOST = 'bigtableclusteradmin.googleapis.com'
"""Cluster Admin API request host."""
CLUSTER_ADMIN_PORT = 443
"""Cluster Admin API request port."""

CLUSTER_STUB_FACTORY = (bigtable_cluster_service_pb2.
                        early_adopter_create_BigtableClusterService_stub)


def _prepare_create_request(cluster):
    """Creates a protobuf request for a CreateCluster request.

    :type cluster: :class:`Cluster`
    :param cluster: The cluster to be created.

    :rtype: :class:`messages_pb2.CreateClusterRequest`
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


def _process_create_response(cluster_response):
    """Processes a create protobuf response.

    :type cluster_response: :class:`bigtable_cluster_data_pb2.Cluster`
    :param cluster_response: The response from a CreateCluster request.

    :rtype: tuple
    :returns: A pair of an integer and datetime stamp. The integer is the ID
              of the operation (``operation_id``) and the timestamp when
              the create operation began (``operation_begin``).
    :raises: :class:`ValueError` if the operation name doesn't match the
             ``_OPERATION_NAME_RE`` regex.
    """
    match = _OPERATION_NAME_RE.match(cluster_response.current_operation.name)
    if match is None:
        raise ValueError('Cluster create operation name was not in the '
                         'expected format.',
                         cluster_response.current_operation.name)
    operation_id = int(match.group('operation_id'))

    request_metadata = _parse_pb_any_to_native(
        cluster_response.current_operation.metadata,
        expected_type=_CLUSTER_CREATE_METADATA)
    operation_begin = _pb_timestamp_to_datetime(
        request_metadata.request_time)

    return operation_id, operation_begin


class Cluster(object):
    """Representation of a Google Cloud Bigtable Cluster.

    We can use a :class:`Cluster` to:

    * :meth:`Cluster.reload` itself
    * :meth:`Cluster.create` itself
    * :meth:`Cluster.delete` itself

    :type zone: string
    :param zone: The name of the zone where the cluster resides.

    :type cluster_id: string
    :param cluster_id: The ID of the cluster.

    :type client: :class:`.client.Client`
    :param client: The client that owns the cluster. Provides
                   authorization and a project ID.

    :type display_name: string
    :param display_name: (Optional) The display name for the cluster in the
                         Cloud Console UI. (Must be between 4 and 30
                         characters.) If this value is not set in the
                         constructor, will fall back to the cluster ID.

    :type serve_nodes: integer
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
        :raises: :class:`ValueError` if the cluster name does not match
                 ``_CLUSTER_NAME_RE`` or if the parsed project ID does
                 not match the project ID on the client.
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

        :rtype: string
        :returns: The project ID stored on the client.
        """
        return self._client

    @property
    def project_id(self):
        """Getter for cluster's project ID.

        :rtype: string
        :returns: The project ID for the cluster (is stored on the client).
        """
        return self._client.project_id

    @property
    def name(self):
        """Cluster name used in requests.

        .. note::
          This property will not change if ``zone`` and ``cluster_id`` do not,
          but the return value is not cached.

        The cluster name is of the form
        "projects/{project_id}/zones/{zone}/clusters/{cluster_id}".

        :rtype: string
        :returns: The cluster name.
        """
        return (self.client.project_name + '/zones/' + self.zone +
                '/clusters/' + self.cluster_id)

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

    def reload(self, timeout_seconds=TIMEOUT_SECONDS):
        """Reload the metadata for this cluster.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        request_pb = messages_pb2.GetClusterRequest(name=self.name)
        stub = make_stub(self.client._credentials, CLUSTER_STUB_FACTORY,
                         CLUSTER_ADMIN_HOST, CLUSTER_ADMIN_PORT)
        with stub:
            response = stub.GetCluster.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_cluster_data_pb2.Cluster`.
            cluster_pb = response.result()

        # NOTE: _update_from_pb does not check that the project, zone and
        #       cluster ID on the response match the request.
        self._update_from_pb(cluster_pb)

    def create(self, timeout_seconds=TIMEOUT_SECONDS):
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

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        request_pb = _prepare_create_request(self)
        stub = make_stub(self.client._credentials, CLUSTER_STUB_FACTORY,
                         CLUSTER_ADMIN_HOST, CLUSTER_ADMIN_PORT)
        with stub:
            response = stub.CreateCluster.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_cluster_data_pb2.Cluster`.
            cluster_pb = response.result()

        self._operation_type = 'create'
        self._operation_id, self._operation_begin = _process_create_response(
            cluster_pb)

    def delete(self, timeout_seconds=TIMEOUT_SECONDS):
        """Delete this cluster.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        request_pb = messages_pb2.DeleteClusterRequest(name=self.name)
        stub = make_stub(self.client._credentials, CLUSTER_STUB_FACTORY,
                         CLUSTER_ADMIN_HOST, CLUSTER_ADMIN_PORT)
        with stub:
            response = stub.DeleteCluster.async(request_pb, timeout_seconds)
            # We expect a `._generated.empty_pb2.Empty`
            response.result()
