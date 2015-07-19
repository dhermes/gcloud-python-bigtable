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

"""Connection to Google Cloud BigTable Table Admin API."""

from gcloud_bigtable._generated import bigtable_table_service_messages_pb2
from gcloud_bigtable.connection import Connection


TABLE_ADMIN_HOST = 'https://bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""
SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
"""Scope for table and cluster API requests."""


class TableConnection(Connection):
    """Connection to Google Cloud BigTable Table API.

    This only allows interacting with tables in a cluster.

    The ``cluster_name`` value must take the form:
        "projects/*/zones/*/clusters/*"
    """

    def create_table(self, cluster_name):
        raise NotImplementedError

    def list_tables(self, cluster_name):
        raise NotImplementedError

    def get_table(self, cluster_name, table_name):
        raise NotImplementedError

    def delete_table(self, cluster_name, table_name):
        raise NotImplementedError

    def rename_table(self, cluster_name, table_name):
        raise NotImplementedError

    def create_column_family(self, cluster_name, table_name):
        raise NotImplementedError

    def update_column_family(self, cluster_name, table_name, column_family):
        raise NotImplementedError

    def delete_column_family(self, cluster_name, table_name, column_family):
        raise NotImplementedError
