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

"""Utility methods for gcloud_bigtable.

Primarily includes helpers for dealing with low-level
protobuf objects.
"""


import datetime
import pytz
import six

from gcloud_bigtable._generated import (
    bigtable_cluster_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._generated import bigtable_cluster_data_pb2 as data_pb2
from gcloud_bigtable._generated import duration_pb2


_TYPE_URL_BASE = 'type.googleapis.com/google.bigtable.'
_ADMIN_TYPE_URL_BASE = _TYPE_URL_BASE + 'admin.cluster.v1.'
_CLUSTER_TYPE_URL = _ADMIN_TYPE_URL_BASE + 'Cluster'
_CLUSTER_CREATE_METADATA = _ADMIN_TYPE_URL_BASE + 'CreateClusterMetadata'
_TYPE_URL_MAP = {
    _CLUSTER_TYPE_URL: data_pb2.Cluster,
    _CLUSTER_CREATE_METADATA: messages_pb2.CreateClusterMetadata,
    _ADMIN_TYPE_URL_BASE + 'UndeleteClusterMetadata': (
        messages_pb2.UndeleteClusterMetadata),
    _ADMIN_TYPE_URL_BASE + 'UpdateClusterMetadata': (
        messages_pb2.UpdateClusterMetadata),
}

EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
# See https://gist.github.com/dhermes/bbc5b7be1932bfffae77
# for appropriate values on other systems.
SSL_CERT_FILE = '/etc/ssl/certs/ca-certificates.crt'


class MetadataTransformer(object):
    """Callable class to transform metadata for gRPC requests.

    :type client: :class:`.client.Client`
    :param client: The client that owns the cluster. Provides authorization and
                   user agent.
    """

    def __init__(self, client):
        self._credentials = client.credentials
        self._user_agent = client.user_agent

    def __call__(self, ignored_val):
        """Adds authorization header to request metadata."""
        access_token = self._credentials.get_access_token().access_token
        return [
            ('Authorization', 'Bearer ' + access_token),
            ('User-agent', self._user_agent),
        ]


class AuthInfo(object):
    """Local namespace for caching auth information."""

    ROOT_CERTIFICATES = None


def _pb_timestamp_to_datetime(timestamp):
    """Convert a Timestamp protobuf to a datetime object.

    :type timestamp: :class:`._generated.timestamp_pb.Timestamp`
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


def _require_pb_property(message_pb, property_name, value):
    """Check that a property agrees with the value on the message.

    :type message_pb: :class:`google.protobuf.message.Message`
    :param message_pb: The message to check for ``property_name``.

    :type property_name: string
    :param property_name: The property value to check against.

    :type value: object or :class:`NoneType <types.NoneType>`
    :param value: The value to check against the cluster. If :data:`None`,
                  will not be checked.

    :rtype: object
    :returns: The value of ``property_name`` set on ``message_pb``.
    :raises: :class:`ValueError` if the result returned from the
             ``message_pb`` does not contain the ``property_name``
             value or if the value returned disagrees with the ``value``
             passed with the request (if that value is not null).
    """
    # Make sure `property_name` is set on the response.
    # NOTE: HasField() doesn't work in protobuf>=3.0.0a3
    all_fields = set([field.name for field in message_pb._fields])
    if property_name not in all_fields:
        raise ValueError('Message does not contain %s.' % (property_name,))
    property_val = getattr(message_pb, property_name)
    if value is None:
        value = property_val
    elif value != property_val:
        raise ValueError('Message returned %s value disagreeing '
                         'with value passed in.' % (property_name,))

    return value


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


def _timedelta_to_duration_pb(timedelta_val):
    """Convert a Python timedelta object to a duration protobuf.

    .. note::

        The Python timedelta has a granularity of microseconds while
        the protobuf duration type has a duration of nanoseconds.

    :type timedelta_val: :class:`datetime.timedelta`
    :param timedelta_val: A timedelta object.

    :rtype: :class:`duration_pb2.Duration`
    :returns: A duration object equivalent to the time delta.
    """
    seconds_decimal = timedelta_val.total_seconds()
    # Truncate the parts other than the integer.
    seconds = int(seconds_decimal)
    if seconds_decimal < 0:
        signed_micros = timedelta_val.microseconds - 10**6
    else:
        signed_micros = timedelta_val.microseconds
    # Convert nanoseconds to microseconds.
    nanos = 1000 * signed_micros
    return duration_pb2.Duration(seconds=seconds, nanos=nanos)


def _duration_pb_to_timedelta(duration_pb):
    """Convert a duration protobuf to a Python timedelta object.

    .. note::

        The Python timedelta has a granularity of microseconds while
        the protobuf duration type has a duration of nanoseconds.

    :type duration_pb: :class:`duration_pb2.Duration`
    :param duration_pb: A protobuf duration object.

    :rtype: :class:`datetime.timedelta`
    :returns: The converted timedelta object.
    """
    return datetime.timedelta(
        seconds=duration_pb.seconds,
        microseconds=(duration_pb.nanos / 1000.0),
    )


def _timestamp_to_microseconds(timestamp, granularity=1000):
    """Converts a native datetime object to microseconds.

    .. note::

        If ``timestamp`` does not have the same timezone as ``EPOCH``
        (which is UTC), then subtracting the epoch from the timestamp
        will raise a :class:`TypeError`.

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


def _to_bytes(value):
    """Converts a value to bytes (or checks that it already is).

    :type value: bytes (or string)
    :param value: The value to ensure is converted to bytes.

    :rtype: bytes
    :returns: The ``value`` as bytes.
    :raises: :class:`TypeError` if the ``value`` is not bytes or string.
    """
    if isinstance(value, six.text_type):
        value = value.encode('utf-8')
    if not isinstance(value, bytes):
        raise TypeError('Row key must be bytes.')
    return value


def _set_certs():
    """Sets the cached root certificates locally."""
    with open(SSL_CERT_FILE, mode='rb') as file_obj:
        AuthInfo.ROOT_CERTIFICATES = file_obj.read()


def set_certs(reset=False):
    """Sets the cached root certificates locally.

    If not manually told to reset or if the value is already set,
    does nothing.

    :type reset: bool
    :param reset: Boolean indicating if the cached certs should be reset.
    """
    if AuthInfo.ROOT_CERTIFICATES is None or reset:
        _set_certs()


def get_certs():
    """Gets the cached root certificates.

    Calls set_certs() first in case the value has not been set, but
    this will do nothing if the value is already set.

    :rtype: string
    :returns: The root certificates set on ``AuthInfo``.
    """
    set_certs(reset=False)
    return AuthInfo.ROOT_CERTIFICATES


def make_stub(client, stub_factory, host, port):
    """Makes a stub for the an API.

    :type client: :class:`.client.Client`
    :param client: The client that owns the cluster. Provides authorization and
                   user agent.

    :type stub_factory: callable
    :param stub_factory: A factory which will create a gRPC stub for
                         a given service.

    :type host: string
    :param host: The host for the service.

    :type port: int
    :param port: The port for the service.

    :rtype: :class:`grpc.early_adopter.implementations._Stub`
    :returns: The stub object used to make gRPC requests to the
              Data API.
    """
    custom_metadata_transformer = MetadataTransformer(client)
    return stub_factory(host, port,
                        metadata_transformer=custom_metadata_transformer,
                        secure=True,
                        root_certificates=get_certs())
