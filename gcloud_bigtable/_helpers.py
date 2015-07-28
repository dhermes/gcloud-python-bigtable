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


def _pb_timestamp_to_datetime(timestamp):
    """Convert a Timestamp protobuf to a datetime object.

    :type timestamp: :class:`._generated.timestamp_pb.Timestamp`
    :param timestamp: A Google returned timestamp protobuf.

    :rtype: :class:`datetime.datetime`
    :returns: A UTC datetime object converted from a protobuf timestamp.
    """
    naive_dt = (
        datetime.datetime.utcfromtimestamp(0) +
        datetime.timedelta(
            seconds=timestamp.seconds,
            microseconds=(timestamp.nanos / 1000.0),
        )
    )
    return naive_dt.replace(tzinfo=pytz.utc)


def _require_pb_property(message_pb, property_name, value):
    """Check that a property agrees with the value on the message.

    :type message_pb: :class:`google.protobuf.message.Message`
    :param message_pb: The message to check for ``property_name``.

    :type property_name: string
    :param property_name: The property value to check against.

    :type value: objector :class:`NoneType`
    :param value: The value to check against the cluster. If ``None``,
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
