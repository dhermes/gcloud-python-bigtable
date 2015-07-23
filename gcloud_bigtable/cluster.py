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

from oauth2client.client import GoogleCredentials
from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import _get_application_default_credential_from_file

from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable._logging import LOGGER
from gcloud_bigtable.cluster_connection import ClusterConnection
from gcloud_bigtable.connection import TIMEOUT_SECONDS


_TYPE_URL_BASE = 'type.googleapis.com/google.bigtable.'
_ADMIN_TYPE_URL_BASE = _TYPE_URL_BASE + 'admin.cluster.v1.'
_CLUSTER_TYPE_URL = _ADMIN_TYPE_URL_BASE + 'Cluster'
_MAX_OPERATION_WAITS = 5
_BASE_OPERATION_WAIT_TIME = 1  # in seconds
_TYPE_URL_MAP = {
    _CLUSTER_TYPE_URL: data_pb2.Cluster,
}


class Cluster(object):
    """Representation of a Google Cloud BigTable Cluster.

    We can use a :class:`Cluster` to:

    * :meth:`Cluster.create` itself
    * :meth:`Cluster.delete` itself

    :type project_id: string
    :param project_id: The ID of the project that owns the cluster.

    :type zone: string
    :param zone: The name of the zone where the cluster will be created.

    :type cluster_id: string
    :param cluster_id: The ID of the cluster to be created.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                       :class:`NoneType`
    :param credentials: The OAuth2 Credentials to use for this cluster.
    """

    def __init__(self, project_id, zone, cluster_id, credentials=None):
        if credentials is None:
            credentials = GoogleCredentials.get_application_default()
        self._credentials = credentials
        self._cluster_conn = ClusterConnection(credentials=self._credentials)

        self.project_id = project_id
        self.zone = zone
        self.cluster_id = cluster_id
        self.display_name = None
        self.serve_nodes = None

    @classmethod
    def from_service_account_json(cls, json_credentials_path,
                                  project_id, zone, cluster_id):
        """Factory to retrieve JSON credentials while creating cluster object.

        :type json_credentials_path: string
        :param json_credentials_path: The path to a private key file (this file
                                      was given to you when you created the
                                      service account). This file must contain
                                      a JSON object with a private key and
                                      other credentials information (downloaded
                                      from the Google APIs console).

        :type project_id: string
        :param project_id: The ID of the project that owns the cluster.

        :type zone: string
        :param zone: The name of the zone where the cluster will be created.

        :type cluster_id: string
        :param cluster_id: The ID of the cluster to be created.

        :rtype: :class:`gcloud_bigtable.Cluster`
        :returns: The cluster object created with the retrieved JSON
                  credentials.
        """
        credentials = _get_application_default_credential_from_file(
            json_credentials_path)
        return cls(project_id, zone, cluster_id, credentials=credentials)

    @classmethod
    def from_service_account_p12(cls, client_email, private_key_path,
                                 project_id, zone, cluster_id):
        """Factory to retrieve P12 credentials while creating cluster object.

        .. note::
          Unless you have an explicit reason to use a PKCS12 key for your
          service account, we recommend using a JSON key.

        :type client_email: string
        :param client_email: The e-mail attached to the service account.

        :type private_key_path: string
        :param private_key_path: The path to a private key file (this file was
                                 given to you when you created the service
                                 account). This file must be in P12 format.

        :type project_id: string
        :param project_id: The ID of the project that owns the cluster.

        :type zone: string
        :param zone: The name of the zone where the cluster will be created.

        :type cluster_id: string
        :param cluster_id: The ID of the cluster to be created.

        :rtype: :class:`gcloud_bigtable.Cluster`
        :returns: The cluster object created with the retrieved P12
                  credentials.
        """
        credentials = SignedJwtAssertionCredentials(
            service_account_name=client_email,
            private_key=_get_contents(private_key_path))
        return cls(project_id, zone, cluster_id, credentials=credentials)

    def create(self, display_name=None, serve_nodes=3,
               timeout_seconds=TIMEOUT_SECONDS):
        """Create this cluster.

        .. note::
          For now, we leave out the arguments ``hdd_bytes`` and ``ssd_bytes``
          (both integers) and also the ``default_storage_type`` (an enum)
          which if not sent will end up as ``data_pb2.STORAGE_SSD``.

        :type display_name: string
        :param display_name: (Optional) The display name for the cluster in
                             the Cloud Console UI.

        :type serve_nodes: integer
        :param serve_nodes: (Optional) The number of nodes in the cluster.
                            Defaults to 3.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        result_pb = self._cluster_conn.create_cluster(
            self.project_id, self.zone, self.cluster_id,
            display_name=display_name, serve_nodes=serve_nodes,
            timeout_seconds=timeout_seconds)

        op_id = _get_operation_id(result_pb.current_operation.name,
                                  self.project_id, self.zone, self.cluster_id)
        op_result_pb = _wait_for_operation(
            self._cluster_conn, self.project_id, self.zone, self.cluster_id,
            op_id, timeout_seconds=timeout_seconds)
        # Make sure the response is a cluster, but don't return it.
        _parse_pb_any_to_native(op_result_pb.response,
                                expected_type=_CLUSTER_TYPE_URL)
        # After successfully parsed response, set the values created.
        self.display_name = display_name
        self.serve_nodes = serve_nodes

    def delete(self, timeout_seconds=TIMEOUT_SECONDS):
        """Delete this cluster.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.
        """
        self._cluster_conn.delete_cluster(
            self.project_id, self.zone, self.cluster_id,
            timeout_seconds=timeout_seconds)
        # After deleting, the config values are no longer set.
        self.display_name = self.serve_nodes = None


def _get_operation_id(operation_name, project_id, zone, cluster_id):
    """Parse a returned name of a long-running operation.

    We expect names to be of the form
        operations/projects/*/zones/*/clusters/*/operations/{OP_ID}

    :type operation_name: string
    :param operation_name: The name of a long-running operation from the
                           Cluster Admin API.

    :type project_id: string
    :param project_id: The ID of the project owning the cluster.

    :type zone: string
    :param zone: The name of the zone owning the cluster.

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
            project_id, zone, cluster_id))
    if expected_op_segment != op_segment:
        raise ValueError('Operation name in unexpected format')

    return int(op_id)


def _wait_for_operation(cluster_connection, project_id, zone, cluster_id,
                        operation_id, timeout_seconds=TIMEOUT_SECONDS):
    """Wait for an operation to complete.

    :type cluster_connection: :class:`.cluster_connection.ClusterConnection`
    :param cluster_connection: Connection to make gRPC requests.

    :type project_id: string
    :param project_id: The ID of the project owning the cluster.

    :type zone: string
    :param zone: The name of the zone owning the cluster.

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
            project_id, zone, cluster_id,
            operation_id, timeout_seconds=timeout_seconds)
        if op_result_pb.done:
            break
        LOGGER.debug('Sleep for %d seconds', sleep_seconds)
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


def _get_contents(filename):
    """Get the contents of a file.

    This is just implemented so we can stub out while testing.

    :type filename: string or bytes
    :param filename: The name of a file to open.

    :rtype: bytes
    :returns: The bytes loaded from the file.
    """
    with open(filename, 'rb') as file_obj:
        return file_obj.read()
