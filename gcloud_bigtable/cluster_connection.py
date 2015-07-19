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

from gcloud_bigtable._generated import bigtable_cluster_service_messages_pb2
from gcloud_bigtable._generated import bigtable_cluster_service_pb2
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import MetadataTransformer
from gcloud_bigtable.connection import get_certs


CLUSTER_STUB_FACTORY = (bigtable_cluster_service_pb2.
                        early_adopter_create_BigtableClusterService_stub)
CLUSTER_ADMIN_HOST = 'bigtableclusteradmin.googleapis.com'
"""Cluster Admin API request host."""
PORT = 443
SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
"""Scope for table and cluster API requests."""


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
    """

    def list_zones(self, project_name):
        """Lists zones associated with project.

        :type project_name: string
        :param project_name: The name of the project to list zones for.

        :raises: :class:`NotImplementedError` always.
        """
        with make_cluster_stub(self._credentials):
            raise NotImplementedError

    def get_cluster(self, project_name, zone_name, cluster_name):
        raise NotImplementedError

    def list_clusters(self, project_name):
        raise NotImplementedError

    def create_cluster(self, project_name, zone_name):
        raise NotImplementedError

    def update_cluster(self, project_name, zone_name, cluster_name):
        raise NotImplementedError

    def delete_cluster(self, project_name, zone_name, cluster_name):
        raise NotImplementedError

    def undelete_cluster(self, project_name, zone_name, cluster_name):
        raise NotImplementedError
