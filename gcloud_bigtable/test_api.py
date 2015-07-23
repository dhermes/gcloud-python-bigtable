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


class Test__get_operation_id(unittest2.TestCase):

    def _callFUT(self, operation_name, project_id, zone_name, cluster_id):
        from gcloud_bigtable.api import _get_operation_id
        return _get_operation_id(operation_name, project_id,
                                 zone_name, cluster_id)

    def test_it(self):
        expected_operation_id = 0
        operation_name = (
            'operations/projects/%s/zones/%s/clusters/%s/operations/%s' % (
                PROJECT_ID, ZONE_NAME, CLUSTER_ID, expected_operation_id))
        operation_id = self._callFUT(operation_name, PROJECT_ID,
                                     ZONE_NAME, CLUSTER_ID)
        self.assertEqual(operation_id, expected_operation_id)

    def test_non_integer_id(self):
        expected_operation_id = 'FOO'
        operation_name = (
            'operations/projects/%s/zones/%s/clusters/%s/operations/%s' % (
                PROJECT_ID, ZONE_NAME, CLUSTER_ID, expected_operation_id))
        with self.assertRaises(ValueError):
            self._callFUT(operation_name, PROJECT_ID,
                          ZONE_NAME, CLUSTER_ID)

    def test_failed_format(self):
        operation_name = 'BAD/FORMAT'
        with self.assertRaises(ValueError):
            self._callFUT(operation_name, PROJECT_ID,
                          ZONE_NAME, CLUSTER_ID)


class Test__wait_for_operation(unittest2.TestCase):

    def _callFUT(self, cluster_connection, project_id, zone_name, cluster_id,
                 operation_id, timeout_seconds=None):
        from gcloud_bigtable.api import _wait_for_operation
        return _wait_for_operation(cluster_connection, project_id, zone_name,
                                   cluster_id, operation_id,
                                   timeout_seconds=timeout_seconds)

    def test_it(self):
        import time
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import api as MUT

        op_not_done = operations_pb2.Operation(done=False)
        op_done = operations_pb2.Operation(done=True)

        connection = _Connection(op_not_done, op_not_done, op_done)
        operation_id = 101
        mock_sleep = _MockCalled()

        with _Monkey(time, sleep=mock_sleep):
            result = self._callFUT(connection, PROJECT_ID, ZONE_NAME,
                                   CLUSTER_ID, operation_id,
                                   timeout_seconds=TIMEOUT_SECONDS)

        self.assertTrue(result is op_done)
        # We expect to sleep twice since first 2 ops are not done.
        wait_time = MUT._BASE_OPERATION_WAIT_TIME
        sleep_args = [(wait_time,), (2 * wait_time,)]
        mock_sleep.check_called(self, sleep_args)
        conn_call = (
            'get_operation',
            (PROJECT_ID, ZONE_NAME, CLUSTER_ID, operation_id),
            {'timeout_seconds': TIMEOUT_SECONDS},
        )
        # We expect 3 calls since first 2 ops are not done.
        conn_called = [conn_call] * 3
        self.assertEqual(connection._called, conn_called)

    def test_timeout(self):
        import time
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import api as MUT

        op_not_done = operations_pb2.Operation(done=False)
        results = [op_not_done] * MUT._MAX_OPERATION_WAITS

        connection = _Connection(*results)
        operation_id = 107
        mock_sleep = _MockCalled()

        with _Monkey(time, sleep=mock_sleep):
            with self.assertRaises(ValueError):
                self._callFUT(connection, PROJECT_ID, ZONE_NAME,
                              CLUSTER_ID, operation_id,
                              timeout_seconds=TIMEOUT_SECONDS)

        # We expect to sleep twice since first 2 ops are not done.
        wait_time = MUT._BASE_OPERATION_WAIT_TIME
        sleep_args = [(wait_time * 2**i,)
                      for i in xrange(MUT._MAX_OPERATION_WAITS)]
        mock_sleep.check_called(self, sleep_args)
        conn_call = (
            'get_operation',
            (PROJECT_ID, ZONE_NAME, CLUSTER_ID, operation_id),
            {'timeout_seconds': TIMEOUT_SECONDS},
        )
        # We expect 3 calls since first 2 ops are not done.
        conn_called = [conn_call] * MUT._MAX_OPERATION_WAITS
        self.assertEqual(connection._called, conn_called)


class _Connection(object):

    def __init__(self, *results):
        self._results = results
        self._called = []

    def create_cluster(self, *args, **kwargs):
        self._called.append(('create_cluster', args, kwargs))
        curr_result = self._results[0]
        self._results = self._results[1:]
        return curr_result

    def get_operation(self, *args, **kwargs):
        self._called.append(('get_operation', args, kwargs))
        curr_result = self._results[0]
        self._results = self._results[1:]
        return curr_result
