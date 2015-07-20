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

"""Connection to Google Cloud BigTable Cluster Admin API."""

from oauth2client.client import AssertionCredentials

from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages)
from gcloud_bigtable._generated import bigtable_cluster_service_pb2
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import MetadataTransformer
from gcloud_bigtable.connection import TIMEOUT_SECONDS
from gcloud_bigtable.connection import get_certs


CLUSTER_STUB_FACTORY = (bigtable_cluster_service_pb2.
                        early_adopter_create_BigtableClusterService_stub)
CLUSTER_ADMIN_HOST = 'bigtableclusteradmin.googleapis.com'
"""Cluster Admin API request host."""
PORT = 443


def make_cluster_stub(credentials):
    """Makes a stub for the Cluster Admin API.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials`
    :param credentials: The OAuth2 Credentials to use for access tokens
                        to authorize requests.

    :rtype: :class:`grpc.early_adopter.implementations._Stub`
    :returns: The stub object used to make gRPC requests to the
              Cluster Admin API.
    """
    custom_metadata_transformer = MetadataTransformer(credentials)
    return CLUSTER_STUB_FACTORY(
        CLUSTER_ADMIN_HOST, PORT,
        metadata_transformer=custom_metadata_transformer,
        secure=True,
        root_certificates=get_certs())


class ClusterConnection(Connection):
    """Connection to Google Cloud BigTable Cluster API.

    This only allows interacting with clusters in a project.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                       :class:`NoneType`
    :param credentials: The OAuth2 Credentials to use for this connection.

    :raises: :class:`TypeError` if the credentials are for a service account.
    """

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
    """Scope for table and cluster API requests."""

    def __init__(self, credentials=None):
        if isinstance(credentials, AssertionCredentials):
            raise TypeError('Service accounts are not able to use the Cluster '
                            'Admin API at this time.')
        super(ClusterConnection, self).__init__(credentials)

    def list_zones(self, project_id, timeout_seconds=TIMEOUT_SECONDS):
        """Lists zones associated with project.

        :type project_id: string
        :param project_id: The ID of the project to list zones for.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: class:`messages.ListZonesResponse`
        :returns: The response object for the list zones request.
        """
        project_name = 'projects/%s' % (project_id,)
        request_pb = messages.ListZonesRequest(name=project_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.ListZones.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def get_cluster(self, project_id, zone_name, cluster_id,
                    timeout_seconds=TIMEOUT_SECONDS):
        """Gets cluster metadata.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone_name: string
        :param zone_name: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster being retrieved.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: class:`messages.Cluster`
        :returns: The response object for the get cluster request.
        """
        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            project_id, zone_name, cluster_id)
        request_pb = messages.GetClusterRequest(name=cluster_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.GetCluster.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def list_clusters(self, project_name):
        """List clusters created in a project."""
        raise NotImplementedError

    def create_cluster(self, project_name, zone_name):
        """Creates a clusters in a project."""
        raise NotImplementedError

    def update_cluster(self, project_name, zone_name, cluster_name):
        """Updates an existing cluster."""
        raise NotImplementedError

    def delete_cluster(self, project_name, zone_name, cluster_name):
        """Deletes a cluster."""
        raise NotImplementedError

    def undelete_cluster(self, project_name, zone_name, cluster_name):
        """Undeletes a cluster that has been queued for deletion."""
        raise NotImplementedError
