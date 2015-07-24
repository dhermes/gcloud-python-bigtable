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

from gcloud_bigtable.connection import Connection


TABLE_ADMIN_HOST = 'https://bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""


class TableConnection(Connection):
    """Connection to Google Cloud Bigtable Table API.

    This only allows interacting with tables in a cluster.

    The ``cluster_name`` value must take the form:
        "projects/*/zones/*/clusters/*"
    """

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
    """Scope for table and cluster API requests."""

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
