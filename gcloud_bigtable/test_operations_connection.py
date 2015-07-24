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


class TestOperationsConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.operations_connection import OperationsConnection
        return OperationsConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        host = 'HOST'
        scope = 'SCOPE'
        connection = self._makeOne(host, scope=scope, credentials=credentials)
        self.assertEqual(connection._host, host)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (scope,))

    def _rpc_method_test_helper(self, rpc_method, method_name):
        from gcloud_bigtable._testing import _Credentials
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable._testing import _StubMock
        from gcloud_bigtable import operations_connection as MUT
        credentials = _Credentials()
        HOST = 'HOST'
        connection = self._makeOne(HOST, credentials=credentials)
        stubs = []
        expected_result = object()

        def mock_make_stub(creds, stub_factory, host, port):
            if host != HOST:  # pragma: NO COVER
                raise ValueError('Unexpected host value: %r' % (host,))
            if port != MUT.PORT:  # pragma: NO COVER
                raise ValueError('Unexpected port value: %r' % (port,))
            if stub_factory != MUT.OPERATIONS_STUB_FACTORY:  # pragma: NO COVER
                raise ValueError('Unexpected stub factory: %r' % (
                    stub_factory,))
            stub = _StubMock(creds, expected_result, method_name)
            stubs.append(stub)
            return stub

        with _Monkey(MUT, make_stub=mock_make_stub):
            result = rpc_method(connection)

        self.assertTrue(result is expected_result)
        return credentials, stubs

    def _check_rpc_stubs_used(self, credentials, stubs, request_type):
        # Asserting length 1 by unpacking.
        stub_used, = stubs
        self.assertTrue(stub_used._credentials is credentials)
        self.assertEqual(stub_used._enter_calls, 1)

        # Asserting length 1 (and a 3-tuple) by unpacking.
        (exc_type, exc_val, _), = stub_used._exit_args
        self.assertTrue(exc_type is None)
        self.assertTrue(isinstance(exc_val, type(None)))

        # Asserting length 1 by unpacking.
        request_obj = stub_used._method
        request_pb, = request_obj.request_pbs
        self.assertTrue(isinstance(request_pb, request_type))
        self.assertEqual(request_obj.request_timeouts, [10])
        return request_pb

    def test_get_operation(self):
        from gcloud_bigtable._generated import operations_pb2

        OPERATION_NAME = 'OPERATION_NAME'

        def rpc_method(connection):
            return connection.get_operation(OPERATION_NAME)

        method_name = 'GetOperation'
        credentials, stubs = self._rpc_method_test_helper(
            rpc_method, method_name)
        request_type = operations_pb2.GetOperationRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb.name, OPERATION_NAME)
        self.assertEqual(len(request_pb._fields), 1)

    def test_list_operations(self):
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import operations_connection as MUT

        DEFAULT_REQUEST = operations_pb2.ListOperationsRequest()
        prepare_list_request_kwargs = []

        def rpc_method(connection):
            return connection.list_operations()

        def mock_prepare_list_request(**kwargs):
            prepare_list_request_kwargs.append(kwargs)
            return DEFAULT_REQUEST

        method_name = 'ListOperations'
        with _Monkey(MUT, _prepare_list_request=mock_prepare_list_request):
            credentials, stubs = self._rpc_method_test_helper(
                rpc_method, method_name)

        request_type = operations_pb2.ListOperationsRequest
        request_pb = self._check_rpc_stubs_used(credentials, stubs,
                                                request_type)
        self.assertEqual(request_pb, DEFAULT_REQUEST)

    def test_cancel_operation(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        host = 'HOST'
        connection = self._makeOne(host, credentials=credentials)
        self.assertRaises(NotImplementedError, connection.cancel_operation)

    def test_delete_operation(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        host = 'HOST'
        connection = self._makeOne(host, credentials=credentials)
        self.assertRaises(NotImplementedError, connection.delete_operation)


class Test__prepare_list_request(unittest2.TestCase):

    def _callFUT(self, filter=None, page_size=None, page_token=None):
        from gcloud_bigtable.operations_connection import _prepare_list_request
        return _prepare_list_request(filter=filter, page_size=page_size,
                                     page_token=page_token)

    def test_defaults(self):
        result = self._callFUT()
        all_fields = set(field.name for field in result._fields.keys())
        self.assertEqual(all_fields, set(['name']))
        self.assertEqual(result.name, 'operations')

    def test_non_default_arguments(self):
        FILTER = 'FILTER'
        PAGE_TOKEN = 'PAGE_TOKEN'
        PAGE_SIZE = 10

        result = self._callFUT(filter=FILTER, page_size=PAGE_SIZE,
                               page_token=PAGE_TOKEN)
        all_fields = set(field.name for field in result._fields.keys())
        self.assertEqual(all_fields,
                         set(['filter', 'name', 'page_token', 'page_size']))
        self.assertEqual(result.filter, FILTER)
        self.assertEqual(result.name, 'operations')
        self.assertEqual(result.page_token, PAGE_TOKEN)
        self.assertEqual(result.page_size, PAGE_SIZE)
