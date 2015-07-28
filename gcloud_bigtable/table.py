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

"""User friendly container for Google Cloud Bigtable Table."""


from gcloud_bigtable._generated import (
    bigtable_table_service_messages_pb2 as messages_pb2)
from gcloud_bigtable._generated import bigtable_table_service_pb2
from gcloud_bigtable.connection import TIMEOUT_SECONDS
from gcloud_bigtable.connection import make_stub


TABLE_STUB_FACTORY = (bigtable_table_service_pb2.
                      early_adopter_create_BigtableTableService_stub)
TABLE_ADMIN_HOST = 'bigtabletableadmin.googleapis.com'
"""Table Admin API request host."""
TABLE_ADMIN_PORT = 443
"""Table Admin API request port."""


class Table(object):
    """Representation of a Google Cloud Bigtable Table.

    .. note::

        We don't define any properties on a table other than the name. As
        the proto says, in a request:

          "The `name` field of the Table and all of its ColumnFamilies must
           be left blank, and will be populated in the response."

        This leaves only the ``current_operation`` and ``granularity``
        fields. The ``current_operation`` is only used for responses while
        ``granularity`` is an enum with only one value.

    We can use a :class:`Table` to:

    * Check if it :meth:`Table.exists`

    :type table_id: string
    :param table_id: The ID of the table.

    :type cluster: :class:`.cluster.Cluster`
    :param cluster: The cluster that owns the table.
    """

    def __init__(self, table_id, cluster):
        self.table_id = table_id
        self._cluster = cluster

    @property
    def cluster(self):
        """Getter for table's cluster.

        :rtype: :class:`.cluster.Cluster`
        :returns: The cluster stored on the table.
        """
        return self._cluster

    @property
    def credentials(self):
        """Getter for table's credentials.

        :rtype: :class:`oauth2client.client.OAuth2Credentials`
        :returns: The credentials stored on the table's client.
        """
        return self._cluster.credentials

    @property
    def name(self):
        """Table name used in requests.

        .. note::
          This property will not change if ``table_id`` does not, but the
          return value is not cached.

        The table name is of the form
        "projects/*/zones/*/clusters/*/tables/{table_id}"

        :rtype: string
        :returns: The table name.
        """
        return self.cluster.name + '/tables/' + self.table_id

    def exists(self, timeout_seconds=TIMEOUT_SECONDS):
        """Reload the metadata for this table.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: boolean
        :returns: Boolean indicating if this table exists. If it does not
                  exist, an exception will be thrown by the API call.
        """
        request_pb = messages_pb2.GetTableRequest(name=self.name)
        stub = make_stub(self.credentials, TABLE_STUB_FACTORY,
                         TABLE_ADMIN_HOST, TABLE_ADMIN_PORT)
        with stub:
            response = stub.GetTable.async(request_pb, timeout_seconds)
            # We expect a `._generated.bigtable_table_data_pb2.Table`
            response.result()

        return True
