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


PROJECT_ID = 'PROJECT_ID'
ZONE_NAME = 'ZONE_NAME'
CLUSTER_ID = 'CLUSTER_ID'
TIMEOUT_SECONDS = 199


class Test_create_cluster(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable.api import create_cluster
        return create_cluster(*args, **kwargs)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import api as MUT

        display_name = 'DISPLAY_NAME'
        serve_nodes = 8
        hdd_bytes = 1337
        ssd_bytes = 42
        operation_id = 77
        op_name = 'OP_NAME'

        create_result = data_pb2.Cluster(
            current_operation=operations_pb2.Operation(
                name=op_name,
            ),
        )
        get_op_result = operations_pb2.Operation()
        connection = _MockWithAttachedMethods(create_result, get_op_result)

        mock_get_operation_id = _MockCalled(operation_id)
        mock_wait_for_operation = _MockCalled(get_op_result)
        final_result = object()
        mock_parse_pb_any_to_native = _MockCalled(final_result)

        with _Monkey(MUT, _get_operation_id=mock_get_operation_id,
                     _wait_for_operation=mock_wait_for_operation,
                     _parse_pb_any_to_native=mock_parse_pb_any_to_native):
            result = self._callFUT(
                connection, PROJECT_ID, ZONE_NAME, CLUSTER_ID,
                display_name=display_name, serve_nodes=serve_nodes,
                hdd_bytes=hdd_bytes, ssd_bytes=ssd_bytes,
                timeout_seconds=TIMEOUT_SECONDS)

        self.assertTrue(result is final_result)
        mock_get_operation_id.check_called(
            self,
            [(op_name, PROJECT_ID, ZONE_NAME, CLUSTER_ID)],
        )
        mock_wait_for_operation.check_called(
            self,
            [(connection, PROJECT_ID, ZONE_NAME, CLUSTER_ID, operation_id)],
            [{'timeout_seconds': TIMEOUT_SECONDS}],
        )
        mock_parse_pb_any_to_native.check_called(
            self,
            [(get_op_result.response,)],
            [{'expected_type': MUT._CLUSTER_TYPE_URL}],
        )
        self.assertEqual(
            connection._called,
            [
                (
                    'create_cluster',
                    (PROJECT_ID, ZONE_NAME, CLUSTER_ID),
                    {
                        'display_name': display_name,
                        'hdd_bytes': hdd_bytes,
                        'serve_nodes': serve_nodes,
                        'ssd_bytes': ssd_bytes,
                        'timeout_seconds': TIMEOUT_SECONDS,
                    },
                ),
            ],
        )
