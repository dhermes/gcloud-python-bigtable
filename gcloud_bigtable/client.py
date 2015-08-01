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

"""Parent client for calling the Google Cloud Bigtable API.

This is the base from which all interactions with the API occur.

In the hierarchy of API concepts

* a :class:`Client` owns a :class:`.Cluster`
* a :class:`.Cluster` owns a :class:`Table <.table.Table>`
* a :class:`Table <.table.Table>` owns a
  :class:`ColumnFamily <.column_family.ColumnFamily>`
* a :class:`Table <.table.Table>` owns a :class:`Row <.row.Row>`
  (and all the cells in the row)
"""


import os
import six
import socket

from oauth2client.client import GoogleCredentials
from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import _get_application_default_credential_from_file

try:
    from google.appengine.api import app_identity
except ImportError:
    app_identity = None

from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._helpers import make_stub
from gcloud_bigtable.cluster import Cluster
from gcloud_bigtable.cluster import CLUSTER_ADMIN_PORT
from gcloud_bigtable.cluster import CLUSTER_ADMIN_HOST
from gcloud_bigtable.cluster import CLUSTER_STUB_FACTORY


ADMIN_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
"""Scope for interacting with the Cluster Admin and Table Admin APIs."""
DATA_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.data'
"""Scope for reading and writing table data."""
READ_ONLY_SCOPE = ('https://www.googleapis.com/auth/'
                   'cloud-bigtable.data.readonly')
"""Scope for reading table data."""

PROJECT_ENV_VAR = 'GCLOUD_PROJECT'
"""Environment variable used to provide an implicit project ID."""

DEFAULT_TIMEOUT_SECONDS = 10
"""The default timeout to use for API requests."""

DEFAULT_USER_AGENT = 'gcloud-bigtable-python'
"""The default user agent for API requests."""


def _project_id_from_environment():
    """Attempts to get the project ID from an environment variable.

    :rtype: string or :class:`NoneType <types.NoneType>`
    :returns: The project ID provided or :data:`None`
    """
    return os.getenv(PROJECT_ENV_VAR)


def _project_id_from_app_engine():
    """Gets the App Engine application ID if it can be inferred.

    :rtype: string or :class:`NoneType <types.NoneType>`
    :returns: App Engine application ID if running in App Engine,
              else :data:`None`.
    """
    if app_identity is None:
        return None

    return app_identity.get_application_id()


def _project_id_from_compute_engine():
    """Gets the Compute Engine project ID if it can be inferred.

    Uses 169.254.169.254 for the metadata server to avoid request
    latency from DNS lookup.

    See https://cloud.google.com/compute/docs/metadata#metadataserver
    for information about this IP address. (This IP is also used for
    Amazon EC2 instances, so the metadata flavor is crucial.)

    See https://github.com/google/oauth2client/issues/93 for context about
    DNS latency.

    :rtype: string or :class:`NoneType <types.NoneType>`
    :returns: Compute Engine project ID if the metadata service is available,
              else :data:`None`.
    """
    host = '169.254.169.254'
    uri_path = '/computeMetadata/v1/project/project-id'
    headers = {'Metadata-Flavor': 'Google'}
    connection = six.moves.http_client.HTTPConnection(host, timeout=0.1)

    try:
        connection.request('GET', uri_path, headers=headers)
        response = connection.getresponse()
        if response.status == 200:
            return response.read()
    except socket.error:  # socket.timeout or socket.error(64, 'Host is down')
        pass
    finally:
        connection.close()


def _determine_project_id(project_id):
    """Determine the project ID from the input or environment.

    When checking the environment, the following precedence is observed:

    * GCLOUD_PROJECT environment variable
    * Google App Engine application ID
    * Google Compute Engine project ID (from metadata server)

    :type project_id: string or :class:`NoneType <types.NoneType>`
    :param project_id: The ID of the project which owns the clusters, tables
                       and data. If not provided, will attempt to
                       determine from the environment.

    :rtype: string
    :returns: The project ID provided or inferred from the environment.
    :raises: :class:`EnvironmentError` if the project ID was not
             passed in and can't be inferred from the environment.
    """
    if project_id is None:
        project_id = _project_id_from_environment()

    if project_id is None:
        project_id = _project_id_from_app_engine()

    if project_id is None:
        project_id = _project_id_from_compute_engine()

    if project_id is None:
        raise EnvironmentError('Project ID was not provided and could not '
                               'be determined from environment.')

    return project_id


class Client(object):
    """Client for interacting with Google Cloud Bigtable API.

    :type credentials:
        :class:`OAuth2Credentials <oauth2client.client.OAuth2Credentials>` or
        :class:`NoneType <types.NoneType>`
    :param credentials: (Optional) The OAuth2 Credentials to use for this
                        cluster. If not provided, defaulst to the Google
                        Application Default Credentials.

    :type project_id: string
    :param project_id: (Optional) The ID of the project which owns the
                       clusters, tables and data. If not provided, will
                       attempt to determine from the environment.

    :type read_only: bool
    :param read_only: (Optional) Boolean indicating if the data scope should be
                      for reading only (or for writing as well). Defaults to
                      :data:`False`.

    :type admin: bool
    :param admin: (Optional) Boolean indicating if the client will be used to
                  interact with the Cluster Admin or Table Admin APIs. This
                  requires the :const:`ADMIN_SCOPE`. Defaults to :data:`False`.

    :type user_agent: string
    :param user_agent: (Optional) The user agent to be used with API request.
                       Defaults to :const:`DEFAULT_USER_AGENT`.

    :type timeout_seconds: int
    :param timeout_seconds: Number of seconds for request time-out. If not
                            passed, defaults to
                            :const:`DEFAULT_TIMEOUT_SECONDS`.

    :raises: :class:`ValueError <exceptions.ValueError>` if both ``read_only``
             and ``admin`` are :data:`True`
    """

    def __init__(self, credentials=None, project_id=None,
                 read_only=False, admin=False, user_agent=DEFAULT_USER_AGENT,
                 timeout_seconds=DEFAULT_TIMEOUT_SECONDS):
        if read_only and admin:
            raise ValueError('A read-only client cannot also perform'
                             'administrative actions.')

        if credentials is None:
            credentials = GoogleCredentials.get_application_default()

        scopes = []
        if read_only:
            scopes.append(READ_ONLY_SCOPE)
        else:
            scopes.append(DATA_SCOPE)

        if admin:
            scopes.append(ADMIN_SCOPE)

        self._credentials = credentials.create_scoped(scopes)
        self._project_id = _determine_project_id(project_id)
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_service_account_json(cls, json_credentials_path, project_id=None,
                                  read_only=False, admin=False):
        """Factory to retrieve JSON credentials while creating client object.

        :type json_credentials_path: string
        :param json_credentials_path: The path to a private key file (this file
                                      was given to you when you created the
                                      service account). This file must contain
                                      a JSON object with a private key and
                                      other credentials information (downloaded
                                      from the Google APIs console).

        :type project_id: string
        :param project_id: The ID of the project which owns the clusters,
                           tables and data. Will be passed to :class:`Client`
                           constructor.

        :type read_only: bool
        :param read_only: Boolean indicating if the data scope should be
                          for reading only (or for writing as well). Will be
                          passed to :class:`Client` constructor.

        :type admin: bool
        :param admin: Boolean indicating if the client will be used to
                      interact with the Cluster Admin or Table Admin APIs. Will
                      be passed to :class:`Client` constructor.

        :rtype: :class:`Client`
        :returns: The client created with the retrieved JSON credentials.
        """
        credentials = _get_application_default_credential_from_file(
            json_credentials_path)
        return cls(credentials=credentials, project_id=project_id,
                   read_only=read_only, admin=admin)

    @classmethod
    def from_service_account_p12(cls, client_email, private_key_path,
                                 project_id=None, read_only=False,
                                 admin=False):
        """Factory to retrieve P12 credentials while creating client object.

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
        :param project_id: The ID of the project which owns the clusters,
                           tables and data. Will be passed to :class:`Client`
                           constructor.

        :type read_only: bool
        :param read_only: Boolean indicating if the data scope should be
                          for reading only (or for writing as well). Will be
                          passed to :class:`Client` constructor.

        :type admin: bool
        :param admin: Boolean indicating if the client will be used to
                      interact with the Cluster Admin or Table Admin APIs. Will
                      be passed to :class:`Client` constructor.

        :rtype: :class:`Client`
        :returns: The client created with the retrieved P12 credentials.
        """
        credentials = SignedJwtAssertionCredentials(
            service_account_name=client_email,
            private_key=_get_contents(private_key_path))
        return cls(credentials=credentials, project_id=project_id,
                   read_only=read_only, admin=admin)

    @property
    def credentials(self):
        """Getter for client's credentials.

        :rtype:
            :class:`OAuth2Credentials <oauth2client.client.OAuth2Credentials>`
        :returns: The credentials stored on the client.
        """
        return self._credentials

    @property
    def project_id(self):
        """Getter for client's project ID.

        :rtype: string
        :returns: The project ID stored on the client.
        """
        return self._project_id

    @property
    def project_name(self):
        """Project name to be used with Cluster Admin API.

        .. note::
          This property will not change if ``project_id`` does not, but the
          return value is not cached.

        The project name is of the form

            ``"projects/{project_id}"``

        :rtype: string
        :returns: The project name to be used with the Cloud Bigtable Admin
                  API RPC service.
        """
        return 'projects/' + self._project_id

    def cluster(self, zone, cluster_id, display_name=None, serve_nodes=3):
        """Factory to create a cluster associated with this client.

        :type zone: string
        :param zone: The name of the zone where the cluster resides.

        :type cluster_id: string
        :param cluster_id: The ID of the cluster.

        :type display_name: string
        :param display_name: (Optional) The display name for the cluster in the
                             Cloud Console UI. (Must be between 4 and 30
                             characters.) If this value is not set in the
                             constructor, will fall back to the cluster ID.

        :type serve_nodes: int
        :param serve_nodes: (Optional) The number of nodes in the cluster.
                            Defaults to 3.

        :rtype: :class:`.Cluster`
        :returns: The cluster owned by this client.
        """
        return Cluster(zone, cluster_id, self,
                       display_name=display_name, serve_nodes=serve_nodes)

    def list_zones(self, timeout_seconds=None):
        """Lists zones associated with project.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on client.

        :rtype: list of strings
        :returns: The names of the zones
        :raises: :class:`ValueError <exceptions.ValueError>` if one of the
                 zones is not in ``OK`` state.
        """
        request_pb = messages_pb2.ListZonesRequest(name=self.project_name)
        stub = make_stub(self, CLUSTER_STUB_FACTORY,
                         CLUSTER_ADMIN_HOST, CLUSTER_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.ListZones.async(request_pb, timeout_seconds)
            # We expect a `messages_pb2.ListZonesResponse`
            list_zones_response = response.result()

        result = []
        for zone in list_zones_response.zones:
            if zone.status != data_pb2.Zone.OK:
                raise ValueError('Zone %s not in OK state' % (
                    zone.display_name,))
            result.append(zone.display_name)
        return result

    def list_clusters(self, timeout_seconds=None):
        """Lists clusters owned by the project.

        :type timeout_seconds: int
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to value set on client.

        :rtype: tuple
        :returns: A pair of results, the first is a list of :class:`.Cluster` s
                  returned and the second is a list of strings (the failed
                  zones in the request).
        """
        request_pb = messages_pb2.ListClustersRequest(name=self.project_name)
        stub = make_stub(self, CLUSTER_STUB_FACTORY,
                         CLUSTER_ADMIN_HOST, CLUSTER_ADMIN_PORT)
        with stub:
            timeout_seconds = timeout_seconds or self.timeout_seconds
            response = stub.ListClusters.async(request_pb, timeout_seconds)
            # We expect a `messages_pb2.ListClustersResponse`
            list_clusters_response = response.result()

        failed_zones = [zone.display_name
                        for zone in list_clusters_response.failed_zones]
        clusters = [Cluster.from_pb(cluster_pb, self)
                    for cluster_pb in list_clusters_response.clusters]
        return clusters, failed_zones


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
