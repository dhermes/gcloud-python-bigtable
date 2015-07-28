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


class _MockCalled(object):

    def __init__(self, result=None):
        self.called_args = []
        self.called_kwargs = []
        self.result = result

    def check_called(self, test_case, args_list, kwargs_list=None):
        test_case.assertEqual(self.called_args, args_list)
        if kwargs_list is None:
            test_case.assertTrue(all([val == {}
                                      for val in self.called_kwargs]))
        else:
            test_case.assertEqual(self.called_kwargs, kwargs_list)

    def __call__(self, *args, **kwargs):
        self.called_args.append(args)
        self.called_kwargs.append(kwargs)
        return self.result


class _AttachedMethod(object):

    def __init__(self, parent, name):
        self.parent = parent
        self.name = name

    def __call__(self, *args, **kwargs):
        self.parent._called.append((self.name, args, kwargs))
        curr_result = self.parent._results[0]
        self.parent._results = self.parent._results[1:]
        return curr_result


class _MockWithAttachedMethods(object):

    def __init__(self, *results):
        self._results = results
        self._called = []

    def __getattr__(self, name):
        # We need not worry about names: _results, _called
        # since __getattribute__ will handle them.
        return _AttachedMethod(self, name)


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
