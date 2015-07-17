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


class Test__print_error(unittest2.TestCase):

    def _callFUT(self, headers, content):
        from gcloud_bigtable import _print_error
        return _print_error(headers, content)

    def test_it(self):
        import gcloud_bigtable as MUT
        original_print_func = MUT._print_func

        values = []

        def mock_print_func(value):
            values.append(value)

        headers = {'foo': 'bar'}
        content = '{"secret": 42}'

        try:
            MUT._print_func = mock_print_func
            self._callFUT(headers, content)
        finally:
            MUT._print_func = original_print_func

        expected_values = [
            'RESPONSE HEADERS:',
            '{\n  "foo": "bar"\n}',
            '-' * 60,
            'RESPONSE BODY:',
            '{\n  "secret": 42\n}',
        ]
        self.assertEqual(values, expected_values)
