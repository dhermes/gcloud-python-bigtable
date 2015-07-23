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

"""User friendly methods for calling the Google Cloud BigTable API."""


from gcloud_bigtable.cluster import _CLUSTER_TYPE_URL
from gcloud_bigtable.cluster import _get_operation_id
from gcloud_bigtable.cluster import _parse_pb_any_to_native
from gcloud_bigtable.cluster import _wait_for_operation
from gcloud_bigtable.connection import TIMEOUT_SECONDS


def create_cluster(cluster_connection, project_id, zone_name, cluster_id,
                   display_name=None, serve_nodes=3, hdd_bytes=None,
                   ssd_bytes=None, timeout_seconds=TIMEOUT_SECONDS):
    """Create a new cluster.

    :type cluster_connection: :class:`.cluster_connection.ClusterConnection`
    :param cluster_connection: Connection to make gRPC requests.

    :type project_id: string
    :param project_id: The ID of the project owning the cluster.

    :type zone_name: string
    :param zone_name: The name of the zone owning the cluster.

    :type cluster_id: string
    :param cluster_id: The name of the cluster being created.

    :type display_name: string
    :param display_name: (Optional) The display name for the cluster in
                         the Cloud Console UI.

    :type serve_nodes: integer
    :param serve_nodes: (Optional) The number of nodes in the cluster.
                        Defaults to 3.

    :type hdd_bytes: integer
    :param hdd_bytes: (Optional) The number of bytes to use for a standard
                      hard drive disk.

    :type ssd_bytes: integer
    :param ssd_bytes: (Optional) The number of bytes to use for a solid
                      state drive.

    :type timeout_seconds: integer
    :param timeout_seconds: Number of seconds for request time-out.
                            If not passed, defaults to ``TIMEOUT_SECONDS``.

    :rtype: :class:`data_pb2.Cluster`
    :returns: The created cluster.
    :raises: :class:`ValueError` if the type URL from the long-running
             operation is not Cluster.
    """
    result_pb = cluster_connection.create_cluster(
        project_id, zone_name, cluster_id, display_name=display_name,
        serve_nodes=serve_nodes, hdd_bytes=hdd_bytes, ssd_bytes=ssd_bytes,
        timeout_seconds=timeout_seconds)

    op_id = _get_operation_id(result_pb.current_operation.name, project_id,
                              zone_name, cluster_id)
    op_result_pb = _wait_for_operation(
        cluster_connection, project_id, zone_name, cluster_id,
        op_id, timeout_seconds=timeout_seconds)
    return _parse_pb_any_to_native(op_result_pb.response,
                                   expected_type=_CLUSTER_TYPE_URL)
