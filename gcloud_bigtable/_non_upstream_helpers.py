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


import datetime
import os
import socket
import sys

import pytz
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

EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)


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


def _timestamp_to_microseconds(timestamp, granularity=1000):
    """Converts a native datetime object to microseconds.

    .. note::

        If ``timestamp`` does not have the same timezone as ``EPOCH``
        (which is UTC), then subtracting the epoch from the timestamp
        will raise a :class:`TypeError <exceptions.TypeError>`.

    :type timestamp: :class:`datetime.datetime`
    :param timestamp: A timestamp to be converted to microseconds.

    :type granularity: int
    :param granularity: The resolution (relative to microseconds) that the
                        timestamp should be truncated to. Defaults to 1000
                        and no other value is likely needed since the only
                        value of the enum
                        :class:`.data_pb2.Table.TimestampGranularity` is
                        :data:`.data_pb2.Table.MILLIS`.

    :rtype: int
    :returns: The ``timestamp`` as microseconds (with the appropriate
              granularity).
    """
    timestamp_seconds = (timestamp - EPOCH).total_seconds()
    timestamp_micros = int(10**6 * timestamp_seconds)
    # Truncate to granularity.
    timestamp_micros -= (timestamp_micros % granularity)
    return timestamp_micros


def _microseconds_to_timestamp(microseconds):
    """Converts microseconds to a native datetime object.

    :type microseconds: int
    :param microseconds: The ``timestamp`` as microseconds.

    :rtype: :class:`datetime.datetime`
    :returns: A timestamp to be converted from microseconds.
    """
    return EPOCH + datetime.timedelta(microseconds=microseconds)


def _to_bytes(value, encoding='ascii'):
    """Converts a string value to bytes, if necessary.

    Unfortunately, ``six.b`` is insufficient for this task since in
    Python2 it does not modify ``unicode`` objects.

    :type value: str / bytes or unicode
    :param value: The string/bytes value to be converted.

    :type encoding: str
    :param encoding: The encoding to use to convert unicode to bytes. Defaults
                     to "ascii", which will not allow any characters from
                     ordinals larger than 127. Other useful values are
                     "latin-1", which which will only allows byte ordinals
                     (up to 255) and "utf-8", which will encode any unicode
                     that needs to be.

    :rtype: str / bytes
    :returns: The original value converted to bytes (if unicode) or as passed
              in if it started out as bytes.
    :raises: :class:`TypeError <exceptions.TypeError>` if the value
             could not be converted to bytes.
    """
    result = (value.encode(encoding)
              if isinstance(value, six.text_type) else value)
    if isinstance(result, six.binary_type):
        return result
    else:
        raise TypeError('%r could not be converted to bytes' % (value,))


def _total_seconds_backport(offset):
    """Backport of timedelta.total_seconds() from python 2.7+.

    :type offset: :class:`datetime.timedelta`
    :param offset: A timedelta object.

    :rtype: int
    :returns: The total seconds (including microseconds) in the
              duration.
    """
    seconds = offset.days * 24 * 60 * 60 + offset.seconds
    return seconds + offset.microseconds * 1e-6


def _total_seconds(offset):
    """Version independent total seconds for a time delta.

    :type offset: :class:`datetime.timedelta`
    :param offset: A timedelta object.

    :rtype: int
    :returns: The total seconds (including microseconds) in the
              duration.
    """
    if sys.version_info[:2] < (2, 7):  # pragma: NO COVER
        return _total_seconds_backport(offset)
    else:
        return offset.total_seconds()


def _pb_timestamp_to_datetime(timestamp):
    """Convert a Timestamp protobuf to a datetime object.

    :type timestamp: :class:`google.protobuf.timestamp_pb2.Timestamp`
    :param timestamp: A Google returned timestamp protobuf.

    :rtype: :class:`datetime.datetime`
    :returns: A UTC datetime object converted from a protobuf timestamp.
    """
    return (
        EPOCH +
        datetime.timedelta(
            seconds=timestamp.seconds,
            microseconds=(timestamp.nanos / 1000.0),
        )
    )
