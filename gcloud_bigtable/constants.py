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

"""Constants for Google Cloud Bigtable API."""


from gcloud_bigtable._generated import bigtable_cluster_service_pb2
from gcloud_bigtable._generated import bigtable_table_service_pb2
from gcloud_bigtable._generated import operations_pb2


TABLE_STUB_FACTORY = (bigtable_table_service_pb2.
                      early_adopter_create_BigtableTableService_stub)
TABLE_ADMIN_HOST = 'bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""
TABLE_ADMIN_PORT = 443
"""Table Admin API request port."""

CLUSTER_STUB_FACTORY = (bigtable_cluster_service_pb2.
                        early_adopter_create_BigtableClusterService_stub)
CLUSTER_ADMIN_HOST = 'bigtableclusteradmin.googleapis.com'
"""Cluster Admin API request host."""
CLUSTER_ADMIN_PORT = 443
"""Cluster Admin API request port."""

OPERATIONS_STUB_FACTORY = operations_pb2.early_adopter_create_Operations_stub
