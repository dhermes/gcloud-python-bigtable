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

    def _grpc_call_helper(self, call_method, method_name, request_obj):
        from gcloud_bigtable._grpc_mocks import StubMockFactory
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import operations_connection as MUT

        host = 'HOST'
        credentials = _MockWithAttachedMethods(False)
        connection = self._makeOne(host, credentials=credentials)

        expected_result = object()
        mock_make_stub = StubMockFactory(expected_result)
        with _Monkey(MUT, make_stub=mock_make_stub):
            result = call_method(connection)

        self.assertTrue(result is expected_result)
        self.assertEqual(credentials._called,
                         [('create_scoped_required', (), {})])

        # Check all the stubs that were created and used as a context
        # manager (should be just one).
        factory_args = (
            credentials,
            MUT.OPERATIONS_STUB_FACTORY,
            host,
            MUT.PORT,
        )
        self.assertEqual(mock_make_stub.factory_calls,
                         [(factory_args, {})])
        stub, = mock_make_stub.stubs  # Asserts just one.
        self.assertEqual(stub._enter_calls, 1)
        self.assertEqual(stub._exit_args,
                         [(None, None, None)])
        # Check all the method calls.
        method_calls = [
            (
                method_name,
                (request_obj, MUT.TIMEOUT_SECONDS),
                {},
            )
        ]
        self.assertEqual(mock_make_stub.method_calls, method_calls)

    def test_get_operation(self):
        from gcloud_bigtable._generated import operations_pb2

        OPERATION_NAME = 'OPERATION_NAME'
        op = operations_pb2.GetOperationRequest(name=OPERATION_NAME)

        def call_method(connection):
            return connection.get_operation(OPERATION_NAME)

        self._grpc_call_helper(call_method, 'GetOperation', op)

    def test_list_operations(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import operations_connection as MUT

        def call_method(connection):
            return connection.list_operations()

        request_obj = object()
        mock_prepare_list_request = _MockCalled(request_obj)
        with _Monkey(MUT, _prepare_list_request=mock_prepare_list_request):
            self._grpc_call_helper(call_method, 'ListOperations', request_obj)

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
