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

"""Helpers needed that are already present in gcloud-python.

They are only needed here, but not needed for merging this upstream.
"""


import os
import socket

import six

from oauth2client.client import GoogleCredentials
from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import _get_application_default_credential_from_file

try:
    from google.appengine.api import app_identity
except ImportError:
    app_identity = None


PROJECT_ENV_VAR = 'GCLOUD_PROJECT'
"""Environment variable used to provide an implicit project ID."""


def _project_from_environment():
    """Attempts to get the project ID from an environment variable.

    :rtype: :class:`str` or :data:`NoneType <types.NoneType>`
    :returns: The project ID provided or :data:`None`
    """
    return os.getenv(PROJECT_ENV_VAR)


def _project_from_app_engine():
    """Gets the App Engine application ID if it can be inferred.

    :rtype: :class:`str` or :data:`NoneType <types.NoneType>`
    :returns: App Engine application ID if running in App Engine,
              else :data:`None`.
    """
    if app_identity is None:
        return None

    return app_identity.get_application_id()


def _project_from_compute_engine():
    """Gets the Compute Engine project ID if it can be inferred.

    Uses 169.254.169.254 for the metadata server to avoid request
    latency from DNS lookup.

    See https://cloud.google.com/compute/docs/metadata#metadataserver
    for information about this IP address. (This IP is also used for
    Amazon EC2 instances, so the metadata flavor is crucial.)

    See https://github.com/google/oauth2client/issues/93 for context about
    DNS latency.

    :rtype: :class:`str` or :data:`NoneType <types.NoneType>`
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


def _determine_project(project=None):
    """Determine the project ID from the input or environment.

    When checking the environment, the following precedence is observed:

    * GCLOUD_PROJECT environment variable
    * Google App Engine application ID
    * Google Compute Engine project ID (from metadata server)

    :type project: str
    :param project: (Optional) The ID of the project which owns the
                    clusters, tables and data. If not provided, will attempt
                    to determine from the environment.

    :rtype: str
    :returns: The project ID provided or inferred from the environment.
    :raises: :class:`EnvironmentError` if the project ID was not
             passed in and can't be inferred from the environment.
    """
    if project is None:
        project = _project_from_environment()

    if project is None:
        project = _project_from_app_engine()

    if project is None:
        project = _project_from_compute_engine()

    if project is None:
        raise EnvironmentError('Project ID was not provided and could not '
                               'be determined from environment.')

    return project


def _get_contents(filename):
    """Get the contents of a file.

    This is just implemented so we can stub out while testing.

    :type filename: :class:`str` or :func:`unicode <unicode>`
    :param filename: The name of a file to open.

    :rtype: bytes
    :returns: The bytes loaded from the file.
    """
    with open(filename, 'rb') as file_obj:
        return file_obj.read()


def get_credentials():
    """Get default credentials."""
    return GoogleCredentials.get_application_default()


class _FactoryMixin(object):
    """Mixin for factories to get credentials."""

    def __init__(self, project=None, credentials=None):
        self.project = _determine_project(project)
        self._credentials = credentials

    @classmethod
    def from_service_account_json(cls, json_credentials_path, *args, **kwargs):
        """Factory to retrieve JSON credentials while creating client object.

        :type json_credentials_path: str
        :param json_credentials_path: The path to a private key file (this file
                                      was given to you when you created the
                                      service account). This file must contain
                                      a JSON object with a private key and
                                      other credentials information (downloaded
                                      from the Google APIs console).

        :type args: tuple
        :param args: Positional arguments.

        :type kwargs: dict
        :param kwargs: Keyword arguments.

        :rtype: :class:`_FactoryMixin`
        :returns: The client created with the retrieved JSON credentials.
        """
        credentials = _get_application_default_credential_from_file(
            json_credentials_path)
        kwargs['credentials'] = credentials
        return cls(*args, **kwargs)

    @classmethod
    def from_service_account_p12(cls, client_email, private_key_path,
                                 *args, **kwargs):
        """Factory to retrieve P12 credentials while creating client object.

        .. note::

            Unless you have an explicit reason to use a PKCS12 key for your
            service account, we recommend using a JSON key.

        :type client_email: str
        :param client_email: The e-mail attached to the service account.

        :type private_key_path: str
        :param private_key_path: The path to a private key file (this file was
                                 given to you when you created the service
                                 account). This file must be in P12 format.

        :type args: tuple
        :param args: Positional arguments.

        :type kwargs: dict
        :param kwargs: Keyword arguments.

        :rtype: :class:`_FactoryMixin`
        :returns: The client created with the retrieved P12 credentials.
        """
        credentials = SignedJwtAssertionCredentials(
            service_account_name=client_email,
            private_key=_get_contents(private_key_path),
            scope=None)
        kwargs['credentials'] = credentials
        return cls(*args, **kwargs)
