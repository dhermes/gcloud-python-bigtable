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
_BASE_OPERATION_WAIT_TIME = 1  # in seconds
_TYPE_URL_MAP = {
    _CLUSTER_TYPE_URL: data_pb2.Cluster,
}


def _get_operation_id(operation_name, project_id, zone_name, cluster_id):
    """Parse a returned name of a long-running operation.

    We expect names to be of the form
        operations/projects/*/zones/*/clusters/*/operations/{OP_ID}

    :type operation_name: string
    :param operation_name: The name of a long-running operation from the
                           Cluster Admin API.

    :type project_id: string
    :param project_id: The ID of the project owning the cluster.

    :type zone_name: string
    :param zone_name: The name of the zone owning the cluster.

    :type cluster_id: string
    :param cluster_id: The name of the cluster.

    :rtype: integer
    :returns: The operation ID (without the cluster, zone or project).
    :raises: :class:`ValueError` if the operation name returned from the
             create request is not the correct format.
    """
    op_segment, op_id = operation_name.rsplit('/', 1)
    expected_op_segment = (
        'operations/projects/%s/zones/%s/clusters/%s/operations' % (
            project_id, zone_name, cluster_id))
    if expected_op_segment != op_segment:
        raise ValueError('Operation name in unexpected format')

    return int(op_id)


def _wait_for_operation(cluster_connection, project_id, zone_name, cluster_id,
                        operation_id, timeout_seconds=TIMEOUT_SECONDS):
    """Wait for an operation to complete.

    :type cluster_connection: :class:`.cluster_connection.ClusterConnection`
    :param cluster_connection: Connection to make gRPC requests.

    :type project_id: string
    :param project_id: The ID of the project owning the cluster.

    :type zone_name: string
    :param zone_name: The name of the zone owning the cluster.

    :type cluster_id: string
    :param cluster_id: The name of the cluster.

    :type operation_id: integer or string
    :param operation_id: The ID of the operation to retrieve.

    :type timeout_seconds: integer
    :param timeout_seconds: Number of seconds for request time-out.
                            If not passed, defaults to ``TIMEOUT_SECONDS``.

    :rtype: :class:`gcloud_bigtable._generated.operations_pb2.Operation`
    :returns: The operation which completed.
    :raises: :class:`ValueError` if the operation never completed.
    """
    wait_count = 0
    sleep_seconds = _BASE_OPERATION_WAIT_TIME
    while wait_count < _MAX_OPERATION_WAITS:
        op_result_pb = cluster_connection.get_operation(
            project_id, zone_name, cluster_id,
            operation_id, timeout_seconds=timeout_seconds)
        if op_result_pb.done:
            break
        time.sleep(sleep_seconds)
        wait_count += 1
        sleep_seconds *= 2  # exponential backoff

    if wait_count == _MAX_OPERATION_WAITS:
        raise ValueError('Long-running operation did not complete.')
    else:
        return op_result_pb


def _parse_pb_any_to_native(any_val, expected_type=None):
    """Convert a serialized "google.protobuf.Any" value to actual type.

    :type any_val: :class:`gcloud_bigtable._generated.any_pb2.Any`
    :param any_val: A serialized protobuf value container.

    :type expected_type: string
    :param expected_type: (Optional) The type URL we expect ``any_val``
                          to have.

    :rtype: object
    :returns: The de-serialized object.
    :raises: :class:`ValueError` if the ``expected_type`` does not match
             the ``type_url`` on the input.
    """
    if expected_type is not None and expected_type != any_val.type_url:
        raise ValueError('Expected type: %s, Received: %s' % (
            expected_type, any_val.type_url))
    container_class = _TYPE_URL_MAP[any_val.type_url]
    return container_class.FromString(any_val.value)


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
