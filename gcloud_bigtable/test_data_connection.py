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


import unittest2

from gcloud_bigtable._grpc_mocks import GRPCMockTestMixin


TABLE_NAME = 'TABLE_NAME'


class TestDataConnection(GRPCMockTestMixin):

    @classmethod
    def setUpClass(cls):
        from gcloud_bigtable import data_connection as MUT
        cls._MUT = MUT
        cls._STUB_FACTORY_NAME = 'DATA_STUB_FACTORY'
        cls._STUB_HOST = MUT.DATA_API_HOST
        cls._STUB_PORT = MUT.PORT

    @classmethod
    def tearDownClass(cls):
        del cls._MUT
        del cls._STUB_FACTORY_NAME
        del cls._STUB_HOST
        del cls._STUB_PORT

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.data_connection import DataConnection
        return DataConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        credentials = object()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)

    def test_read_rows(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import data_connection as MUT

        request_obj = object()
        mock_prepare_read = _MockCalled(request_obj)

        def call_method(connection):
            return connection.read_rows(TABLE_NAME)

        with _Monkey(MUT, _prepare_read=mock_prepare_read):
            self._grpc_call_helper(call_method, 'ReadRows', request_obj)

        mock_prepare_read.check_called(
            self,
            [(TABLE_NAME,)],
            [{
                'allow_row_interleaving': None,
                'filter_': None,
                'num_rows_limit': None,
                'row_key': None,
                'row_range': None,
            }],
        )

    def test_sample_row_keys(self):
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)

        request_obj = messages_pb2.SampleRowKeysRequest(table_name=TABLE_NAME)

        def call_method(connection):
            return connection.sample_row_keys(TABLE_NAME)

        self._grpc_call_helper(call_method, 'SampleRowKeys', request_obj)

    def test_mutate_row(self):
        credentials = object()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError, connection.mutate_row,
                          table_name, row_key)

    def test_check_and_mutate_row(self):
        credentials = object()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError, connection.check_and_mutate_row,
                          table_name, row_key)

    def test_read_modify_write_row(self):
        credentials = object()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError,
                          connection.read_modify_write_row,
                          table_name, row_key)


class Test__prepare_read(unittest2.TestCase):

    def _callFUT(self, table_name, row_key=None, row_range=None,
                 filter_=None, allow_row_interleaving=None,
                 num_rows_limit=None):
        from gcloud_bigtable.data_connection import _prepare_read
        return _prepare_read(
            table_name, row_key=row_key, row_range=row_range, filter_=filter_,
            allow_row_interleaving=allow_row_interleaving,
            num_rows_limit=num_rows_limit)

    def test_defaults(self):
        read_request = self._callFUT(TABLE_NAME)
        all_fields = set(field.name for field in read_request._fields.keys())
        self.assertEqual(all_fields, set(['table_name']))
        self.assertEqual(read_request.table_name, TABLE_NAME)

    def test_both_row_key_and_row_range(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        row_key = b'foobar'
        row_range = data_pb2.RowRange(
            start_key=b'baz',
            end_key=b'quux',
        )
        with self.assertRaises(ValueError):
            self._callFUT(TABLE_NAME, row_key=row_key, row_range=row_range)

    def test_non_default_arguments_excluding_row_range(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        row_key = b'foobar'
        filter_ = data_pb2.RowFilter(row_sample_filter=0.25)
        allow_row_interleaving = True
        num_rows_limit = 1001
        read_request = self._callFUT(
            TABLE_NAME, row_key=row_key, filter_=filter_,
            allow_row_interleaving=allow_row_interleaving,
            num_rows_limit=num_rows_limit)

        all_fields = set(field.name for field in read_request._fields.keys())
        self.assertEqual(all_fields, set([
            'allow_row_interleaving',
            'filter',
            'num_rows_limit',
            'row_key',
            'table_name',
        ]))
        self.assertEqual(read_request.table_name, TABLE_NAME)
        self.assertEqual(read_request.allow_row_interleaving,
                         allow_row_interleaving)
        self.assertEqual(read_request.filter, filter_)
        self.assertEqual(read_request.num_rows_limit, num_rows_limit)
        self.assertEqual(read_request.row_key, row_key)

    def test_non_default_arguments_only_row_range(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        row_range = data_pb2.RowRange(
            start_key=b'baz',
            end_key=b'quux',
        )
        read_request = self._callFUT(TABLE_NAME, row_range=row_range)

        all_fields = set(field.name for field in read_request._fields.keys())
        self.assertEqual(all_fields, set([
            'row_range',
            'table_name',
        ]))
        self.assertEqual(read_request.table_name, TABLE_NAME)
        self.assertEqual(read_request.row_range, row_range)
