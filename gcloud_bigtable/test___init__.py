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


class TestGRPCImportFailure(unittest2.TestCase):

    _MODULES_TO_EDIT = ('gcloud_bigtable', 'grpc',
                        'grpc._adapter', 'grpc._adapter._c')

    @classmethod
    def setUpClass(cls):
        import imp
        mod_name = 'gcloud_bigtable'
        # H/T: http://pymotw.com/2/imp/
        cls._load_module_args = (mod_name,) + imp.find_module(mod_name)

    @classmethod
    def tearDownClass(cls):
        del cls._load_module_args

    def _module_patch_helper(self, function_to_test):
        import sys

        removed_mods = {}
        for mod_name in self._MODULES_TO_EDIT:
            removed_mods[mod_name] = sys.modules.get(mod_name)

        try:
            sys.modules.pop('gcloud_bigtable', None)
            # Test method should re-import gcloud_bigtable
            function_to_test()
        finally:
            for mod_name, value in removed_mods.items():
                sys.modules[mod_name] = value

    def test_success(self):
        import imp
        import sys
        import types

        TEST_CASE = self

        def function_to_test():
            c_mod = types.ModuleType('grpc._adapter._c')
            sys.modules['grpc._adapter._c'] = c_mod

            adapter_mod = types.ModuleType('grpc._adapter')
            adapter_mod._c = c_mod
            sys.modules['grpc._adapter'] = adapter_mod

            grpc_mod = types.ModuleType('grpc')
            grpc_mod._adapter = adapter_mod
            sys.modules['grpc'] = grpc_mod

            # Re-import gcloud_bigtable and check our custom _c module.
            gcloud_bigtable = imp.load_module(*TEST_CASE._load_module_args)
            TEST_CASE.assertTrue(gcloud_bigtable._c is c_mod)

        self._module_patch_helper(function_to_test)

    @staticmethod
    def _create_fake_grpc_adapter_package(contents):
        import os
        import tempfile

        temp_dir = tempfile.mkdtemp()

        curr_dir = os.path.join(temp_dir, 'grpc')
        os.mkdir(curr_dir)
        with open(os.path.join(curr_dir, '__init__.py'), 'wb') as file_obj:
            file_obj.write(b'')

        curr_dir = os.path.join(curr_dir, '_adapter')
        os.mkdir(curr_dir)
        with open(os.path.join(curr_dir, '__init__.py'), 'wb') as file_obj:
            file_obj.write(b'')

        filename = os.path.join(curr_dir, '_c.py')
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)

        return temp_dir

    def _import_fail_helper(self, orig_exc_message):
        import imp
        import sys

        # Be sure the message contains libgrpc.so
        c_module_contents = 'raise ImportError(%r)\n' % (orig_exc_message,)
        c_module_contents = c_module_contents.encode('ascii')
        temp_dir = self._create_fake_grpc_adapter_package(c_module_contents)

        TEST_CASE = self

        def function_to_test():
            sys.path.insert(0, temp_dir)
            try:
                sys.modules.pop('grpc')
                sys.modules.pop('grpc._adapter')
                sys.modules.pop('grpc._adapter._c')

                try:
                    imp.load_module(*TEST_CASE._load_module_args)
                except ImportError as exc:
                    if 'libgrpc.so' in orig_exc_message:
                        TEST_CASE.assertNotEqual(str(exc), orig_exc_message)
                    else:
                        TEST_CASE.assertEqual(str(exc), orig_exc_message)
            finally:
                sys.path.remove(temp_dir)

        self._module_patch_helper(function_to_test)

    def test_system_library_cause(self):
        # Be sure the message contains libgrpc.so
        orig_exc_message = 'bad libgrpc.so'
        self._import_fail_helper(orig_exc_message)

    def test_non_system_library_cause(self):
        # Be sure the message does not contain libgrpc.so
        orig_exc_message = 'Other cause of error'
        self._import_fail_helper(orig_exc_message)
