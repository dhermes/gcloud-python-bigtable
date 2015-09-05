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


class TestConnection(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.happybase.connection import Connection
        return Connection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        with self.assertRaises(NotImplementedError):
            self._makeOne()

    def test_constructor_no_autoconnect(self):
        connection = self._makeOne(autoconnect=False)
        self.assertEqual(connection.timeout, None)
        self.assertEqual(connection.table_prefix, None)
        self.assertEqual(connection.table_prefix_separator, '_')

    def test_constructor_explicit(self):
        timeout = object()
        table_prefix = 'table-prefix'
        table_prefix_separator = 'sep'

        connection = self._makeOne(
            autoconnect=False, timeout=timeout,
            table_prefix=table_prefix,
            table_prefix_separator=table_prefix_separator)
        self.assertEqual(connection.timeout, timeout)
        self.assertEqual(connection.table_prefix, table_prefix)
        self.assertEqual(connection.table_prefix_separator,
                         table_prefix_separator)

    def test_constructor_non_string_prefix(self):
        table_prefix = object()

        with self.assertRaises(TypeError):
            self._makeOne(autoconnect=False,
                          table_prefix=table_prefix)

    def test_constructor_non_string_prefix_separator(self):
        table_prefix_separator = object()

        with self.assertRaises(TypeError):
            self._makeOne(autoconnect=False,
                          table_prefix_separator=table_prefix_separator)

    def test_constructor_with_host(self):
        with self.assertRaises(ValueError):
            self._makeOne(host=object())

    def test_constructor_with_port(self):
        with self.assertRaises(ValueError):
            self._makeOne(port=object())

    def test_constructor_with_compat(self):
        with self.assertRaises(ValueError):
            self._makeOne(compat=object())

    def test_constructor_with_transport(self):
        with self.assertRaises(ValueError):
            self._makeOne(transport=object())

    def test_constructor_with_protocol(self):
        with self.assertRaises(ValueError):
            self._makeOne(protocol=object())

    def test_open(self):
        connection = self._makeOne(autoconnect=False)
        with self.assertRaises(NotImplementedError):
            connection.open()

    def test_close(self):
        connection = self._makeOne(autoconnect=False)
        with self.assertRaises(NotImplementedError):
            connection.close()

    def test_table(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        use_prefix = False
        with self.assertRaises(NotImplementedError):
            connection.table(name, use_prefix=use_prefix)

    def test_tables(self):
        connection = self._makeOne(autoconnect=False)
        with self.assertRaises(NotImplementedError):
            connection.tables()

    def test_create_table(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        families = {}
        with self.assertRaises(NotImplementedError):
            connection.create_table(name, families)

    def test_delete_table(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        disable = True
        with self.assertRaises(NotImplementedError):
            connection.delete_table(name, disable=disable)

    def test_enable_table(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        with self.assertRaises(NotImplementedError):
            connection.enable_table(name)

    def test_disable_table(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        with self.assertRaises(NotImplementedError):
            connection.disable_table(name)

    def test_is_table_enabled(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        with self.assertRaises(NotImplementedError):
            connection.is_table_enabled(name)

    def test_compact_table(self):
        connection = self._makeOne(autoconnect=False)
        name = 'table-name'
        major = True
        with self.assertRaises(NotImplementedError):
            connection.compact_table(name, major=major)
