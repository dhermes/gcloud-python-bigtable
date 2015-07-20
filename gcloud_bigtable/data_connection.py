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

"""Connection to Google Cloud BigTable Data API."""

from gcloud_bigtable.connection import Connection


DATA_API_HOST = 'bigtable.googleapis.com'
"""Base URL for API requests."""


class DataConnection(Connection):
    """Connection to Google Cloud BigTable Data API.

    This only allows interacting with data in an existing table.

    The ``table_name`` value must take the form:
        "projects/*/zones/*/clusters/*/tables/*"
    """

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.data'
    """Scope for data API requests."""

    READ_ONLY_SCOPE = ('https://www.googleapis.com/auth/'
                       'cloud-bigtable.data.readonly')
    """Read-only scope for data API requests."""

    def read_rows(self, table_name, row_key=None, row_range=None,
                  filter=None, allow_row_interleaving=None,
                  num_rows_limit=None):
        """Read rows from table.

        Streams back the contents of all requested rows, optionally applying
        the same Reader filter to each. Depending on their size, rows may be
        broken up across multiple responses, but atomicity of each row will
        still be preserved.

        :type table_name: string
        :param table_name: The unique name of the table from which to read.

        :type row_key: bytes
        :param row_key: (Optional) The key of a single row from which to read.

        :type row_range: :class:`bigtable_data_pb2.RowRange`
        :param row_range: (Optional) A range of rows from which to read.

        :type filter: :class:`bigtable_data_pb2.RowFilter`
        :param filter: (Optional) The filter to apply to the contents of the
                       specified row(s). If unset, reads the entire table.

        :type allow_row_interleaving: boolean
        :param allow_row_interleaving: (Optional) By default, rows are read
                                       sequentially, producing results which
                                       are guaranteed to arrive in increasing
                                       row order. Setting
                                       "allow_row_interleaving" to true allows
                                       multiple rows to be
                                       interleaved in the response stream,
                                       which increases throughput but breaks
                                       this guarantee, and may force the client
                                       to use more memory to buffer
                                       partially-received rows.

        :type num_rows_limit: integer
        :param num_rows_limit: (Optional) The read will terminate after
                               committing to N rows' worth of results. The
                               default (zero) is to return all results. Note
                               that if "allow_row_interleaving" is set to true,
                               partial results may be returned for more than N
                               rows. However, only N "commit_row" chunks will
                               be sent.

        :rtype: :class:`bigtable_service_messages_pb2.ReadRowsResponse`
        :returns: The response returned by the backend.
        :raises: :class:`NotImplementedError` always.
        """
        raise NotImplementedError

    def sample_row_keys(self, table_name):
        """Samples row keys."""
        raise NotImplementedError

    def mutate_row(self, table_name, row_key):
        """Mutates a row."""
        raise NotImplementedError

    def check_and_mutate_row(self, table_name, row_key):
        """Checks and mutates a row."""
        raise NotImplementedError

    def read_modify_write_row(self, table_name, row_key):
        """Reads, modifies and writes a row."""
        raise NotImplementedError
