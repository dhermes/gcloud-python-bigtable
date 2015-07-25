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


_CLUSTER_NAME_RE = re.compile(r'^projects/(?P<project_id>[^/]+)/'
                              r'zones/(?P<zone>[^/]+)/clusters/'
                              r'(?P<cluster_id>[a-z][-a-z0-9]*)$')


class Cluster(object):
    """Representation of a Google Cloud Bigtable Cluster.

    :type zone: string
    :param zone: The name of the zone where the cluster resides.

    :type cluster_id: string
    :param cluster_id: The ID of the cluster.

    :type client: :class:`.client.Client`
    :param client: The client that owns the cluster. Provides
                   authorization and a project ID.
    """

    def __init__(self, zone, cluster_id, client):
        self.zone = zone
        self.cluster_id = cluster_id
        self._client = client

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

        return cls(match.group('zone'), match.group('cluster_id'), client)

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
        return (other.zone == self.zone and
                other.cluster_id == self.cluster_id and
                other.client == self.client)

    def __ne__(self, other):
        return not self.__eq__(other)
