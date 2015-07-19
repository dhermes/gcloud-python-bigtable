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

"""Shared testing utilities."""


class _Credentials(object):

    _scopes = None

    @staticmethod
    def create_scoped_required():
        return True

    def create_scoped(self, scope):
        self._scopes = scope
        return self


class _StubMock(object):

    def __init__(self, credentials):
        self._credentials = credentials
        self._enter_calls = 0
        self._exit_args = []

    def __enter__(self):
        self._enter_calls += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_args.append((exc_type, exc_val, exc_tb))


class _StubMockResponse(object):

    def __init__(self, stub, result):
        self.stub = stub
        self._result = result

    def result(self):
        return self._result

class _Monkey(object):
    # context-manager for replacing module names in the scope of a test.

    def __init__(self, module, **kw):
        self.module = module
        self.to_restore = dict([(key, getattr(module, key)) for key in kw])
        for key, value in kw.items():
            setattr(module, key, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key, value in self.to_restore.items():
            setattr(self.module, key, value)
