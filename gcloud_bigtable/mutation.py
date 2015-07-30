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

"""User friendly container for Google Cloud Bigtable Mutation."""


import datetime
import pytz
import six

from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2


_EPOCH = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)


class Mutation(object):
    """Accumulator class for Bigtable cell mutations.

    Expected to be used in ``MutateRow`` and ``CheckAndMutateRow`` requests.
    """

    def __init__(self):
        self._pb_mutations = []

    def set(self, column_family_id, column_name, value, timestamp=None):
        """Sets a value in this mutation.

        :type column_family_id: string
        :param column_family_id: The column family that the value is being
                                 added to.

        :type column_name: bytes (or string)
        :param column_name: The column within the column family that the value
                            is being added to.

        :type value: bytes
        :param value: The value to set in the cell.

        :type timestamp: :class:`datetime.datetime`
        :param timestamp: (Optional) The timestamp of the operation.

        :raises: :class:`TypeError` if the ``value`` is not bytes.
        """
        if isinstance(column_name, six.text_type):
            column_name = column_name.encode('utf-8')
        if not isinstance(value, bytes):
            raise TypeError('Value for a cell must be bytes.')
        if timestamp is None:
            # Use -1 for current Bigtable server time.
            timestamp_micros = -1
        else:
            timestamp_seconds = (timestamp - _EPOCH).total_seconds()
            timestamp_micros = int(10**6 * timestamp_seconds)

        mutation_val = data_pb2.Mutation.SetCell(
            family_name=column_family_id,
            column_qualifier=column_name,
            timestamp_micros=timestamp_micros,
            value=value,
        )
        mutation_pb = data_pb2.Mutation(set_cell=mutation_val)
        self._pb_mutations.append(mutation_pb)
