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
