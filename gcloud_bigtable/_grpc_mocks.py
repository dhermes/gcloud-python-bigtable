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


class _StubMock(object):

    def __init__(self, *results):
        self.results = results
        self.method_calls = []

    def __getattr__(self, name):
        # We need not worry about attributes set in constructor
        # since __getattribute__ will handle them.
        return MethodMock(name, self)
