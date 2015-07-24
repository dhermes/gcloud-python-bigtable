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

"""Connection to Google Cloud Bigtable Table Admin API."""

from gcloud_bigtable._generated import bigtable_table_service_pb2
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import MetadataTransformer
from gcloud_bigtable.connection import get_certs


TABLE_STUB_FACTORY = (bigtable_table_service_pb2.
                      early_adopter_create_BigtableTableService_stub)
TABLE_ADMIN_HOST = 'bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""
PORT = 443


def make_table_stub(credentials):
    """Makes a stub for the Table Admin API.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials`
    :param credentials: The OAuth2 Credentials to use for access tokens
                        to authorize requests.

    :rtype: :class:`grpc.early_adopter.implementations._Stub`
    :returns: The stub object used to make gRPC requests to the
              Table Admin API.
    """
    custom_metadata_transformer = MetadataTransformer(credentials)
    return TABLE_STUB_FACTORY(
        TABLE_ADMIN_HOST, PORT,
        metadata_transformer=custom_metadata_transformer,
        secure=True,
        root_certificates=get_certs())


class TableConnection(Connection):
    """Connection to Google Cloud Bigtable Table API.

    This only allows interacting with tables in a cluster.

    The ``cluster_name`` value must take the form:
        "projects/*/zones/*/clusters/*"
    """

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
    """Scope for Table Admin and Cluster Admin API requests."""

    def create_table(self, cluster_name):
        """Create a table in an existing cluster."""
        raise NotImplementedError

    def list_tables(self, cluster_name):
        """List tables in an existing cluster."""
        raise NotImplementedError

    def get_table(self, cluster_name, table_name):
        """Get table metadata."""
        raise NotImplementedError

    def delete_table(self, cluster_name, table_name):
        """Delete a table."""
        raise NotImplementedError

    def rename_table(self, cluster_name, table_name):
        """Rename a table."""
        raise NotImplementedError

    def create_column_family(self, cluster_name, table_name):
        """Create a column family in a table."""
        raise NotImplementedError

    def update_column_family(self, cluster_name, table_name, column_family):
        """Update an existing column family in a table."""
        raise NotImplementedError

    def delete_column_family(self, cluster_name, table_name, column_family):
        """Delete an existing column family in a table."""
        raise NotImplementedError
