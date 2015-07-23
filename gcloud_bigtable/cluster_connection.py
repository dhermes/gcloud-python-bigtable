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

from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._generated import bigtable_cluster_service_pb2
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import MetadataTransformer
from gcloud_bigtable.connection import TIMEOUT_SECONDS
from gcloud_bigtable.connection import get_certs
from gcloud_bigtable.operations_connection import OperationsConnection


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


def _prepare_cluster(name=None, display_name=None, serve_nodes=3):
    """Create a cluster object with many optional arguments.

    .. note::
      For now, we leave out the arguments ``hdd_bytes`` and ``ssd_bytes``
      (both integers) and also the ``default_storage_type`` (an enum)
      which if not sent will end up as ``data_pb2.STORAGE_SSD``.

    :type name: string
    :param name: (Optional). The name of the cluster. Must be of the form
                 "projects/*/zones/*/clusters/*".

    :type display_name: string
    :param display_name: (Optional) The display name for the cluster in
                         the Cloud Console UI.

    :type serve_nodes: integer
    :param serve_nodes: (Optional) The number of nodes in the cluster.
                        Defaults to 3.

    :rtype: :class:`data_pb2.Cluster`
    :returns: The cluster object required.
    """
    cluster_kwargs = {}

    if name is not None:
        cluster_kwargs['name'] = name

    if display_name is not None:
        cluster_kwargs['display_name'] = display_name

    cluster_kwargs['serve_nodes'] = serve_nodes
    return data_pb2.Cluster(**cluster_kwargs)


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
        # We assume credentials.create_scoped_required() will be False
        # so that the same credentials can be used.
        self._ops_connection = OperationsConnection(
            CLUSTER_ADMIN_HOST, scope=self.SCOPE, credentials=credentials)

    def get_operation(self, project_id, zone, cluster_id,
                      operation_id, timeout_seconds=TIMEOUT_SECONDS):
        """Gets a long-running operation.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone: string
        :param zone: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster that initiatied the
                           long-running operation.

        :type operation_id: integer or string
        :param operation_id: The ID of the operation to retrieve.

        :type timeout_seconds: integer
        :param timeout_seconds: (Optional) Number of seconds for request
                                time-out. If not passed, defaults to
                                ``TIMEOUT_SECONDS``.

        :rtype: :class:`gcloud_bigtable._generated.operations_pb2.Operation`
        :returns: The operation retrieved.
        """
        operation_name = (
            'operations/projects/%s/zones/%s/clusters/%s/operations/%s' % (
                project_id, zone, cluster_id, operation_id))
        return self._ops_connection.get_operation(
            operation_name, timeout_seconds=timeout_seconds)

    def list_zones(self, project_id, timeout_seconds=TIMEOUT_SECONDS):
        """Lists zones associated with project.

        :type project_id: string
        :param project_id: The ID of the project to list zones for.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`messages_pb2.ListZonesResponse`
        :returns: The response object for the list zones request.
        """
        project_name = 'projects/%s' % (project_id,)
        request_pb = messages_pb2.ListZonesRequest(name=project_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.ListZones.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def get_cluster(self, project_id, zone, cluster_id,
                    timeout_seconds=TIMEOUT_SECONDS):
        """Gets cluster metadata.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone: string
        :param zone: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster being retrieved.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`data_pb2.Cluster`
        :returns: The response object for the get cluster request.
        """
        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            project_id, zone, cluster_id)
        request_pb = messages_pb2.GetClusterRequest(name=cluster_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.GetCluster.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def list_clusters(self, project_id, timeout_seconds=TIMEOUT_SECONDS):
        """List clusters created in a project.

        :type project_id: string
        :param project_id: The ID of the project to list clusters for.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`messages_pb2.ListClustersResponse`
        :returns: The response object for the list clusters request.
        """
        project_name = 'projects/%s' % (project_id,)
        request_pb = messages_pb2.ListClustersRequest(name=project_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.ListClusters.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def create_cluster(self, project_id, zone, cluster_id,
                       display_name=None, serve_nodes=3,
                       timeout_seconds=TIMEOUT_SECONDS):
        """Create a new cluster.

        .. note::
          For now, we leave out the arguments ``hdd_bytes`` and ``ssd_bytes``
          (both integers) and also the ``default_storage_type`` (an enum)
          which if not sent will end up as ``data_pb2.STORAGE_SSD``.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone: string
        :param zone: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster being created.

        :type display_name: string
        :param display_name: (Optional) The display name for the cluster in
                             the Cloud Console UI.

        :type serve_nodes: integer
        :param serve_nodes: (Optional) The number of nodes in the cluster.
                            Defaults to 3.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`data_pb2.Cluster`
        :returns: The response object for the create cluster request.
        """
        zone_full_name = 'projects/%s/zones/%s' % (project_id, zone)
        cluster = _prepare_cluster(display_name=display_name,
                                   serve_nodes=serve_nodes)

        # From the .proto definition of CreateClusterRequest: the "name",
        # "delete_time", and "current_operation" fields must be left blank.
        request_pb = messages_pb2.CreateClusterRequest(
            name=zone_full_name,
            cluster_id=cluster_id,
            cluster=cluster,
        )
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.CreateCluster.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def update_cluster(self, project_id, zone, cluster_id,
                       display_name=None, serve_nodes=3,
                       timeout_seconds=TIMEOUT_SECONDS):
        """Updates an existing cluster.

        .. note::
          For now, we leave out the arguments ``hdd_bytes`` and ``ssd_bytes``
          (both integers) and also the ``default_storage_type`` (an enum)
          which if not sent will end up as ``data_pb2.STORAGE_SSD``.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone: string
        :param zone: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster being updated.

        :type display_name: string
        :param display_name: (Optional) The display name for the cluster in
                             the Cloud Console UI.

        :type serve_nodes: integer
        :param serve_nodes: (Optional) The number of nodes in the cluster.
                            Defaults to 3.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`data_pb2.Cluster`
        :returns: The updated cluster from the update cluster request.
        """
        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            project_id, zone, cluster_id)
        cluster = _prepare_cluster(name=cluster_name,
                                   display_name=display_name,
                                   serve_nodes=serve_nodes)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.UpdateCluster.async(cluster, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def delete_cluster(self, project_id, zone, cluster_id,
                       timeout_seconds=TIMEOUT_SECONDS):
        """Deletes a cluster.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone: string
        :param zone: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster being deleted.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`gcloud_bigtable._generated.empty_pb2.Empty`
        :returns: The empty response object for the delete cluster request.
        """
        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            project_id, zone, cluster_id)
        request_pb = messages_pb2.DeleteClusterRequest(name=cluster_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.DeleteCluster.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def undelete_cluster(self, project_id, zone, cluster_id,
                         timeout_seconds=TIMEOUT_SECONDS):
        """Undeletes a cluster that has been queued for deletion.

        :type project_id: string
        :param project_id: The ID of the project owning the cluster.

        :type zone: string
        :param zone: The name of the zone owning the cluster.

        :type cluster_id: string
        :param cluster_id: The name of the cluster being retrieved.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`gcloud_bigtable._generated.operations_pb2.Operation`
        :returns: The long running operation that will perform the undelete.
        """
        cluster_name = 'projects/%s/zones/%s/clusters/%s' % (
            project_id, zone, cluster_id)
        request_pb = messages_pb2.UndeleteClusterRequest(name=cluster_name)
        result_pb = None
        with make_cluster_stub(self._credentials) as stub:
            response = stub.UndeleteCluster.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb
