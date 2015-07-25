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

"""Connection to Google Cloud Bigtable Data API."""


from gcloud_bigtable._generated import bigtable_service_pb2
from gcloud_bigtable._generated import (
    bigtable_service_messages_pb2 as messages_pb2)
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import TIMEOUT_SECONDS
from gcloud_bigtable.connection import make_stub


DATA_STUB_FACTORY = (bigtable_service_pb2.
                     early_adopter_create_BigtableService_stub)
DATA_API_HOST = 'bigtable.googleapis.com'
"""Data API request host."""
PORT = 443


def _prepare_read(table_name, row_key=None, row_range=None,
                  filter_=None, allow_row_interleaving=None,
                  num_rows_limit=None):
    """Prepare a :class:`messages_pb2.ReadRowsRequest`.

    .. note::

      At most one of ``row_key`` and ``row_range`` can be set.

    :type table_name: string
    :param table_name: The name of the table we are reading from.
                       Must be of the form
                       "projects/../zones/../clusters/../tables/.."
                       Since this is a low-level class, we don't check
                       this, rather we expect callers to pass correctly
                       formatted data.

    :type row_key: bytes
    :param row_key: (Optional) The key of a single row from which to read.

    `._generated.bigtable_data_pb2.
    :type row_range: :class:`._generated.bigtable_data_pb2.RowRange`
    :param row_range: (Optional) A range of rows from which to read.

    :type filter_: :class:`._generated.bigtable_data_pb2.RowFilter`
    :param filter_: (Optional) The filter to apply to the contents of the
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
    :raises: :class:`ValueError` if both ``row_key`` and ``row_range``
             are passed in.
    """
    request_kwargs = {'table_name': table_name}
    # NOTE: oneof, target{row_key, row_range}
    if row_key is not None and row_range is not None:
        raise ValueError('At most one of `row_key` and `row_range` '
                         'can be set')
    if row_key is not None:
        request_kwargs['row_key'] = row_key
    if row_range is not None:
        request_kwargs['row_range'] = row_range
    if filter_ is not None:
        request_kwargs['filter'] = filter_
    if allow_row_interleaving is not None:
        request_kwargs['allow_row_interleaving'] = allow_row_interleaving
    if num_rows_limit is not None:
        request_kwargs['num_rows_limit'] = num_rows_limit

    return messages_pb2.ReadRowsRequest(**request_kwargs)


class DataConnection(Connection):
    """Connection to Google Cloud Bigtable Data API.

    Enables interaction with data in an existing table.
    """

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.data'
    """Scope for data API requests."""

    READ_ONLY_SCOPE = ('https://www.googleapis.com/auth/'
                       'cloud-bigtable.data.readonly')
    """Read-only scope for data API requests."""

    def read_rows(self, table_name, row_key=None, row_range=None,
                  filter_=None, allow_row_interleaving=None,
                  num_rows_limit=None, timeout_seconds=TIMEOUT_SECONDS):
        """Read rows from table.

        Streams back the contents of all requested rows, optionally applying
        the same Reader filter to each. Depending on their size, rows may be
        broken up across multiple responses, but atomicity of each row will
        still be preserved.

        .. note::

          If neither ``row_key`` nor ``row_range`` is set, reads from all rows.
          Otherwise, at most one of ``row_key`` and ``row_range`` can be set.

        :type table_name: string
        :param table_name: The name of the table we are reading from.
                           Must be of the form
                           "projects/../zones/../clusters/../tables/.."
                           Since this is a low-level class, we don't check
                           this, rather we expect callers to pass correctly
                           formatted data.

        :type row_key: bytes
        :param row_key: (Optional) The key of a single row from which to read.

        :type row_range: :class:`._generated.bigtable_data_pb2.RowRange`
        :param row_range: (Optional) A range of rows from which to read.

        :type filter_: :class:`._generated.bigtable_data_pb2.RowFilter`
        :param filter_: (Optional) The filter to apply to the contents of the
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

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`bigtable_service_messages_pb2.ReadRowsResponse`
        :returns: The response returned by the backend.
        """
        request_pb = _prepare_read(
            table_name, row_key=row_key, row_range=row_range, filter_=filter_,
            allow_row_interleaving=allow_row_interleaving,
            num_rows_limit=num_rows_limit)
        result_pb = None
        stub = make_stub(self._credentials, DATA_STUB_FACTORY,
                         DATA_API_HOST, PORT)
        with stub:
            response = stub.ReadRows.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def sample_row_keys(self, table_name, timeout_seconds=TIMEOUT_SECONDS):
        """Returns a sample of row keys in the table.

        The returned row keys will delimit contiguous sections of the table of
        approximately equal size, which can be used to break up the data for
        distributed tasks like mapreduces.

        :type table_name: string
        :param table_name: The name of the table we are taking the sample from.
                           Must be of the form
                           "projects/../zones/../clusters/../tables/.."
                           Since this is a low-level class, we don't check
                           this, rather we expect callers to pass correctly
                           formatted data.

        :type timeout_seconds: integer
        :param timeout_seconds: Number of seconds for request time-out.
                                If not passed, defaults to ``TIMEOUT_SECONDS``.

        :rtype: :class:`messages_pb2.SampleRowKeysResponse`
        :returns: The sample row keys response returned.
        """
        request_pb = messages_pb2.SampleRowKeysRequest(table_name=table_name)
        result_pb = None
        stub = make_stub(self._credentials, DATA_STUB_FACTORY,
                         DATA_API_HOST, PORT)
        with stub:
            response = stub.SampleRowKeys.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def mutate_row(self, table_name, row_key):
        """Mutates a row."""
        raise NotImplementedError

    def check_and_mutate_row(self, table_name, row_key):
        """Checks and mutates a row."""
        raise NotImplementedError

    def read_modify_write_row(self, table_name, row_key):
        """Reads, modifies and writes a row."""
        raise NotImplementedError
