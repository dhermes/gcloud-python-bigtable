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

"""Testing mocks for grpc."""


import unittest2


class AsyncResult(object):
    """Result returned from a ``MethodMock.async`` call."""

    def __init__(self, result):
        self._result = result

    def result(self):
        """Result method on an asyc object."""
        return self._result


class MethodMock(object):
    """Mock for :class:`grpc.framework.alpha._reexport._UnaryUnarySyncAsync`.

    May need to be callable and needs to (in our use) have an
    ``async`` method.
    """

    def __init__(self, name, factory):
        self._name = name
        self._factory = factory

    def async(self, *args, **kwargs):
        """Async method meant to mock a gRPC stub request."""
        self._factory.method_calls.append((self._name, args, kwargs))
        curr_result = self._factory.results[0]
        self._factory.results = self._factory.results[1:]
        return AsyncResult(curr_result)


class StubMock(object):
    """Mock for :class:`grpc.early_adopter.implementations._Stub`.

    Needs to
    - Implement a context manager (and potentially track the calls
      to ``__enter__`` and ``__exit__``).
    - Return properties (via ``__getattr__``) that are mocks for
      :class:`grpc.framework.alpha._reexport._UnaryUnarySyncAsync`.
      We implement these with :class:`_MethodMethod`.
    """

    def __init__(self, factory):
        self._factory = factory
        self._enter_calls = 0
        self._exit_args = []

    def __getattr__(self, name):
        # We need not worry about attributes set in constructor
        # since __getattribute__ will handle them.
        return MethodMock(name, self._factory)

    def __enter__(self):
        self._enter_calls += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_args.append((exc_type, exc_val, exc_tb))


class StubMockFactory(object):
    """Factory for :class:`StubMock`.

    Takes a fixed set of results (as positional arguments) and
    then passes those results along to each stub created.
    """

    def __init__(self, *results):
        self.results = results
        self.factory_calls = []
        self.method_calls = []
        self.stubs = []

    def __call__(self, *args, **kwargs):
        self.factory_calls.append((args, kwargs))
        stub = StubMock(self)
        self.stubs.append(stub)
        return stub


class GRPCMockTestMixin(unittest2.TestCase):
    """Mix-in to allow easy mocking for gRPC methods.

    Expects :func:`gcloud_bigtable.connection.make_stub` to be used
    for making API calls.

    Also expects that :meth:`setUpClass` will be called and the following
    class attributes will be set:

    * ``_MUT``: The module under test.
    * ``_STUB_SCOPES``: Scopes used by ``MUT`` when creating a client.
    * ``_STUB_FACTORY_NAME``: Name of default factory used by ``MUT`` to create
                              a gRPC stub.
    * ``_STUB_HOST``: Host used by ``MUT`` to create a gRPC stub.
    * ``_STUB_PORT``: Port used by ``MUT`` to create a gRPC stub.

    The final three will all be arguments to ``make_stub``.

    We use the name of the stub factory rather than the function itself because
    Python tries too hard. If we store a function on a class, then accessing it
    from an instance returns a "bound method", rather than the value we want.
    """

    def _grpc_call_helper(self, call_method, method_name, request_obj,
                          stub_factory=None):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey

        credentials = _MockWithAttachedMethods(False)
        connection = self._makeOne(credentials=credentials)

        expected_result = object()
        mock_make_stub = StubMockFactory(expected_result)
        with _Monkey(self._MUT, make_stub=mock_make_stub):
            result = call_method(connection)

        self.assertTrue(result is expected_result)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])

        # Check all the stubs that were created and used as a context
        # manager (should be just one).
        factory_args = (
            credentials,
            stub_factory or getattr(self._MUT, self._STUB_FACTORY_NAME),
            self._STUB_HOST,
            self._STUB_PORT,
        )
        self.assertEqual(mock_make_stub.factory_calls,
                         [(factory_args, {})])
        self.assertEqual(len(mock_make_stub.stubs), 1)
        stub = mock_make_stub.stubs[0]
        self.assertEqual(stub._enter_calls, 1)
        self.assertEqual(stub._exit_args,
                         [(None, None, None)])
        # Check all the method calls.
        method_calls = [
            (
                method_name,
                (request_obj, self._MUT.TIMEOUT_SECONDS),
                {},
            )
        ]
        self.assertEqual(mock_make_stub.method_calls, method_calls)

    def _grpc_client_test_helper(self, method_name, result_method, request_pb,
                                 response_pb, expected_result, project_id,
                                 stub_factory=None):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable.client import Client

        # Create the client.
        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(credentials, project_id=project_id)

        # Create mocks to avoid HTTP/2 calls.
        mock_make_stub = StubMockFactory(response_pb)

        # Call the method with the mocks.
        with _Monkey(self._MUT, make_stub=mock_make_stub):
            result = result_method(client)
        self.assertEqual(result, expected_result)

        self.assertEqual(credentials._called, [
            ('create_scoped', (self._STUB_SCOPES,), {}),
        ])
        factory_args = (
            scoped_creds,
            stub_factory or getattr(self._MUT, self._STUB_FACTORY_NAME),
            self._STUB_HOST,
            self._STUB_PORT,
        )
        self.assertEqual(mock_make_stub.factory_calls,
                         [(factory_args, {})])
        self.assertEqual(mock_make_stub.method_calls, [
            (
                method_name,
                (request_pb, self._MUT.TIMEOUT_SECONDS),
                {},
            ),
        ])
        self.assertEqual(len(mock_make_stub.stubs), 1)
        stub = mock_make_stub.stubs[0]
        self.assertEqual(stub._enter_calls, 1)
        self.assertEqual(stub._exit_args,
                         [(None, None, None)])
