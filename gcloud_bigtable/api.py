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


import time

from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable.connection import TIMEOUT_SECONDS


_TYPE_URL_BASE = 'type.googleapis.com/google.bigtable.'
_ADMIN_TYPE_URL_BASE = _TYPE_URL_BASE + 'admin.cluster.v1.'
_CLUSTER_TYPE_URL = _ADMIN_TYPE_URL_BASE + 'Cluster'
_MAX_OPERATION_WAITS = 5
_OPERATION_WAIT_TIME = 5  # in seconds


def create_cluster(cluster_connection, project_id, zone_name, cluster_id,
                   display_name=None, serve_nodes=3, hdd_bytes=None,
                   ssd_bytes=None, timeout_seconds=TIMEOUT_SECONDS):
    """Create a new cluster.

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

    :rtype: :class:`messages.Cluster`
    :returns: The created cluster.
    :raises: :class:`ValueError` if the operation name returned from the
             create request is not the correct format or if the type URL
             from the long-running operation is not Cluster.
    """
    result_pb = cluster_connection.create_cluster(
        project_id, zone_name, cluster_id, display_name=display_name,
        serve_nodes=serve_nodes, hdd_bytes=hdd_bytes, ssd_bytes=ssd_bytes,
        timeout_seconds=timeout_seconds)

    op_segment, op_id = result_pb.current_operation.name.rsplit('/', 1)
    expected_op_segment = (
        'operations/projects/%s/zones/%s/clusters/%s/operations' % (
            project_id, zone_name, cluster_id))
    if expected_op_segment != op_segment:
        raise ValueError('Operation name in unexpected format')

    op_id = int(op_id)
    op_result_pb = cluster_connection.get_operation(
        project_id, zone_name, cluster_id,
        op_id, timeout_seconds=timeout_seconds)

    wait_count = 0
    while wait_count < _MAX_OPERATION_WAITS and not op_result_pb.done:
        wait_count += 1
        time.sleep(_OPERATION_WAIT_TIME)
        op_result_pb = cluster_connection.get_operation(
            project_id, zone_name, cluster_id,
            op_id, timeout_seconds=timeout_seconds)

    if op_result_pb.response.type_url != _CLUSTER_TYPE_URL:
        raise ValueError('Unexpected type URL for response in '
                         'long-running operation.')

    return data_pb2.Cluster.FromString(op_result_pb.response.value)
