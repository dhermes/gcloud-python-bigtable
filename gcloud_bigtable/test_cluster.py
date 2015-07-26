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
ZONE = 'ZONE'
CLUSTER_ID = 'CLUSTER_ID'
TIMEOUT_SECONDS = 199


class TestCluster(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.cluster import Cluster
        return Cluster

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_default(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT
        from gcloud_bigtable.cluster_connection import ClusterConnection

        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        credentials_result = _MockWithAttachedMethods(False)
        # Expected order of calls (on creds class):
        # - get_application_default
        mock_creds_class = _MockWithAttachedMethods(credentials_result)

        with _Monkey(MUT, GoogleCredentials=mock_creds_class):
            cluster = self._makeOne(PROJECT_ID, ZONE, CLUSTER_ID)

        self.assertEqual(cluster.project_id, PROJECT_ID)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        self.assertEqual(cluster._credentials, credentials_result)
        self.assertTrue(isinstance(cluster._cluster_conn, ClusterConnection))
        self.assertEqual(cluster._cluster_conn._credentials,
                         credentials_result)
        self.assertEqual(mock_creds_class._called,
                         [('get_application_default', (), {})])
        self.assertEqual(credentials_result._called, [
            ('create_scoped_required', (), {}),
        ])

    def test_constructor_explicit_credentials(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.cluster_connection import ClusterConnection

        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        credentials = _MockWithAttachedMethods(False)
        cluster = self._makeOne(PROJECT_ID, ZONE, CLUSTER_ID,
                                credentials=credentials)

        self.assertEqual(cluster.project_id, PROJECT_ID)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        self.assertEqual(cluster._credentials, credentials)
        self.assertTrue(isinstance(cluster._cluster_conn, ClusterConnection))
        self.assertEqual(cluster._cluster_conn._credentials, credentials)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])

    def test_from_service_account_json(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT
        from gcloud_bigtable.cluster_connection import ClusterConnection

        klass = self._getTargetClass()
        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        credentials = _MockWithAttachedMethods(False)
        get_adc = _MockCalled(credentials)
        json_credentials_path = 'JSON_CREDENTIALS_PATH'

        with _Monkey(MUT,
                     _get_application_default_credential_from_file=get_adc):
            cluster = klass.from_service_account_json(
                json_credentials_path, PROJECT_ID, ZONE, CLUSTER_ID)

        self.assertEqual(cluster.project_id, PROJECT_ID)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        self.assertEqual(cluster._credentials, credentials)
        self.assertTrue(isinstance(cluster._cluster_conn, ClusterConnection))
        self.assertEqual(cluster._cluster_conn._credentials, credentials)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])
        # _get_application_default_credential_from_file only has pos. args.
        get_adc.check_called(self, [(json_credentials_path,)])

    def test_from_service_account_p12(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT
        from gcloud_bigtable.cluster_connection import ClusterConnection

        klass = self._getTargetClass()
        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        credentials = _MockWithAttachedMethods(False)
        signed_creds = _MockCalled(credentials)
        private_key = 'PRIVATE_KEY'
        mock_get_contents = _MockCalled(private_key)
        client_email = 'CLIENT_EMAIL'
        private_key_path = 'PRIVATE_KEY_PATH'

        with _Monkey(MUT, SignedJwtAssertionCredentials=signed_creds,
                     _get_contents=mock_get_contents):
            cluster = klass.from_service_account_p12(
                client_email, private_key_path, PROJECT_ID, ZONE, CLUSTER_ID)

        self.assertEqual(cluster.project_id, PROJECT_ID)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        self.assertEqual(cluster._credentials, credentials)
        self.assertTrue(isinstance(cluster._cluster_conn, ClusterConnection))
        self.assertEqual(cluster._cluster_conn._credentials, credentials)
        self.assertEqual(credentials._called, [
            ('create_scoped_required', (), {}),
        ])
        # SignedJwtAssertionCredentials() called with only kwargs
        signed_creds_kw = {
            'private_key': private_key,
            'service_account_name': client_email,
        }
        signed_creds.check_called(self, [()], [signed_creds_kw])
        # Load private key (via _get_contents) from the key path.
        mock_get_contents.check_called(self, [(private_key_path,)])

    def test_reload(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        # - create_scoped_required (via OperationsConnection)
        credentials = _MockWithAttachedMethods(False, False)
        # The credentials will just be thrown away when we
        # patch cluster._cluster_conn.
        cluster = self._makeOne(PROJECT_ID, ZONE, CLUSTER_ID,
                                credentials=credentials)
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        # Patch the connection with a mock.
        get_cluster_result = object()
        cluster._cluster_conn = connection = _MockWithAttachedMethods(
            get_cluster_result)
        pb_property_result = object()
        mock_require_pb_property = _MockCalled(pb_property_result)

        with _Monkey(MUT, _require_pb_property=mock_require_pb_property):
            cluster.reload(timeout_seconds=TIMEOUT_SECONDS)

        # Make sure reload sets the config.
        self.assertEqual(cluster.display_name, pb_property_result)
        self.assertEqual(cluster.serve_nodes, pb_property_result)

        mock_require_pb_property.check_called(
            self,
            [
                (get_cluster_result, 'display_name', None),
                (get_cluster_result, 'serve_nodes', None),
            ],
        )
        self.assertEqual(connection._called, [
            (
                'get_cluster',
                (PROJECT_ID, ZONE, CLUSTER_ID),
                {'timeout_seconds': TIMEOUT_SECONDS},
            ),
        ])

    def _create_or_update_helper(self, method_name, display_name, serve_nodes):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        # - create_scoped_required (via OperationsConnection)
        credentials = _MockWithAttachedMethods(False, False)
        # The credentials will just be thrown away when we
        # patch cluster._cluster_conn.
        cluster = self._makeOne(PROJECT_ID, ZONE, CLUSTER_ID,
                                credentials=credentials)
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        operation_id = 77
        op_name = 'OP_NAME'

        # Set-up request to return from a mock connection.
        api_action_result = data_pb2.Cluster(
            current_operation=operations_pb2.Operation(
                name=op_name,
            ),
        )
        get_op_result = operations_pb2.Operation()
        # Patch the connection with a mock.
        cluster._cluster_conn = connection = _MockWithAttachedMethods(
            api_action_result)
        # Create mocks for the three helpers.
        mock_get_operation_id = _MockCalled(operation_id)
        mock_wait_for_operation = _MockCalled(get_op_result)
        parse_pb_result = object()
        mock_parse_pb_any_to_native = _MockCalled(parse_pb_result)
        mock_require_pb_property = _MockCalled(serve_nodes)

        with _Monkey(MUT, _get_operation_id=mock_get_operation_id,
                     _wait_for_operation=mock_wait_for_operation,
                     _parse_pb_any_to_native=mock_parse_pb_any_to_native,
                     _require_pb_property=mock_require_pb_property):
            method = getattr(cluster, method_name)
            method(display_name=display_name, serve_nodes=serve_nodes,
                   timeout_seconds=TIMEOUT_SECONDS)

        self.assertEqual(cluster.display_name, display_name)
        self.assertEqual(cluster.serve_nodes, serve_nodes)

        mock_get_operation_id.check_called(
            self,
            [(op_name, PROJECT_ID, ZONE, CLUSTER_ID)],
        )
        mock_wait_for_operation.check_called(
            self,
            [(connection, PROJECT_ID, ZONE, CLUSTER_ID, operation_id)],
            [{'timeout_seconds': TIMEOUT_SECONDS}],
        )
        mock_parse_pb_any_to_native.check_called(
            self,
            [(get_op_result.response,)],
            [{'expected_type': MUT._CLUSTER_TYPE_URL}],
        )
        mock_require_pb_property.check_called(
            self,
            [(parse_pb_result, 'serve_nodes', serve_nodes)],
        )
        self.assertEqual(
            connection._called,
            [
                (
                    '%s_cluster' % (method_name,),
                    (PROJECT_ID, ZONE, CLUSTER_ID),
                    {
                        'display_name': display_name,
                        'serve_nodes': serve_nodes,
                        'timeout_seconds': TIMEOUT_SECONDS,
                    },
                ),
            ],
        )

    def test_create(self):
        display_name = 'DISPLAY_NAME'
        serve_nodes = 8
        self._create_or_update_helper('create', display_name, serve_nodes)

    def test_update(self):
        display_name = 'DISPLAY_NAME'
        serve_nodes = 8
        self._create_or_update_helper('update', display_name, serve_nodes)

    def test_update_fallback_to_display_name(self):
        serve_nodes = 8
        self._create_or_update_helper('update', None, serve_nodes)

    def test_update_fallback_to_serve_nodes(self):
        display_name = 'DISPLAY_NAME'
        self._create_or_update_helper('update', display_name, None)

    def test_update_without_updates(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        # - create_scoped_required (via OperationsConnection)
        credentials = _MockWithAttachedMethods(False, False)
        # The credentials will just be thrown away when we
        # patch cluster._cluster_conn.
        cluster = self._makeOne(PROJECT_ID, ZONE, CLUSTER_ID,
                                credentials=credentials)

        # Patch the connection with a mock that has **no** responses.
        cluster._cluster_conn = connection = _MockWithAttachedMethods()
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        cluster.update()
        # Ensure that update did not call any methods on the connection.
        self.assertEqual(connection._called, [])

    def test_delete(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods

        # Expected order of calls (on creds):
        # - create_scoped_required (via ClusterConnection)
        # - create_scoped_required (via OperationsConnection)
        credentials = _MockWithAttachedMethods(False, False)
        # The credentials will just be thrown away when we
        # patch cluster._cluster_conn.
        cluster = self._makeOne(PROJECT_ID, ZONE, CLUSTER_ID,
                                credentials=credentials)
        cluster.display_name = object()
        cluster.serve_nodes = object()

        # Patch the connection with a mock.
        delete_result = object()
        cluster._cluster_conn = connection = _MockWithAttachedMethods(
            delete_result)
        cluster.delete(timeout_seconds=TIMEOUT_SECONDS)
        # Make sure delete removes the config.
        self.assertEqual(cluster.display_name, None)
        self.assertEqual(cluster.serve_nodes, None)

        self.assertEqual(connection._called, [
            (
                'delete_cluster',
                (PROJECT_ID, ZONE, CLUSTER_ID),
                {'timeout_seconds': TIMEOUT_SECONDS},
            ),
        ])


class Test__get_operation_id(unittest2.TestCase):

    def _callFUT(self, operation_name, project_id, zone, cluster_id):
        from gcloud_bigtable.cluster import _get_operation_id
        return _get_operation_id(operation_name, project_id,
                                 zone, cluster_id)

    def test_it(self):
        expected_operation_id = 0
        operation_name = (
            'operations/projects/%s/zones/%s/clusters/%s/operations/%s' % (
                PROJECT_ID, ZONE, CLUSTER_ID, expected_operation_id))
        operation_id = self._callFUT(operation_name, PROJECT_ID,
                                     ZONE, CLUSTER_ID)
        self.assertEqual(operation_id, expected_operation_id)

    def test_non_integer_id(self):
        expected_operation_id = 'FOO'
        operation_name = (
            'operations/projects/%s/zones/%s/clusters/%s/operations/%s' % (
                PROJECT_ID, ZONE, CLUSTER_ID, expected_operation_id))
        with self.assertRaises(ValueError):
            self._callFUT(operation_name, PROJECT_ID,
                          ZONE, CLUSTER_ID)

    def test_failed_format(self):
        operation_name = 'BAD/FORMAT'
        with self.assertRaises(ValueError):
            self._callFUT(operation_name, PROJECT_ID,
                          ZONE, CLUSTER_ID)


class Test__log_operation_duration(unittest2.TestCase):

    def _callFUT(self, operation_pb):
        from gcloud_bigtable.cluster import _log_operation_duration
        return _log_operation_duration(operation_pb)

    def test_logger_not_debug(self):
        import logging
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        mock_logger = _MockWithAttachedMethods()
        # Make it larger than DEBUG so it is not in DEBUG mode.
        mock_logger.level = logging.DEBUG + 1
        with _Monkey(MUT, LOGGER=mock_logger):
            self._callFUT(None)

        # We make sure that nothing is logged in this case.
        self.assertEqual(mock_logger._called, [])

    def _logger_test_helper(self, seconds_before, nanos_before,
                            seconds_after, nanos_after,
                            seconds_delta, nanos_delta):
        import logging
        from gcloud_bigtable._generated import any_pb2
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._generated import timestamp_pb2
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        type_url_display = 'TYPE_URL'
        # Need a period at the beginning.
        type_url = 'FOO.' + type_url_display
        fake_url_map = {type_url: messages_pb2.CreateClusterMetadata}

        metadata_pb = messages_pb2.CreateClusterMetadata(
            request_time=timestamp_pb2.Timestamp(
                seconds=seconds_before,
                nanos=nanos_before,
            ),
            finish_time=timestamp_pb2.Timestamp(
                seconds=seconds_after,
                nanos=nanos_after,
            ),
        )
        operation_pb = operations_pb2.Operation(
            metadata=any_pb2.Any(
                type_url=type_url,
                value=metadata_pb.SerializeToString(),
            )
        )

        debug_response = object()
        mock_logger = _MockWithAttachedMethods(debug_response)
        mock_logger.level = logging.DEBUG
        with _Monkey(MUT, LOGGER=mock_logger, _TYPE_URL_MAP=fake_url_map):
            self._callFUT(operation_pb)

        self.assertEqual(mock_logger._called, [
            (
                'debug',
                (
                    MUT._DURATION_LOG_TEMPLATE,
                    type_url_display,
                    seconds_delta,
                    nanos_delta,
                ),
                {},
            )
        ])

    def test_logger_in_debug_mode(self):
        seconds = 101
        nanos_before = 42
        nanos_after = 1337
        nanos_delta = nanos_after - nanos_before
        seconds_delta = 0
        self._logger_test_helper(seconds, nanos_before, seconds,
                                 nanos_after, seconds_delta, nanos_delta)

    def test_logger_in_debug_mode_differing_seconds(self):
        seconds_before = 1
        seconds_after = 2
        nanos_before = 10**9 - 1
        nanos_after = 1
        # The above is rigged so that the difference is 2.
        nanos_delta = 2
        seconds_delta = 0
        self._logger_test_helper(seconds_before, nanos_before, seconds_after,
                                 nanos_after, seconds_delta, nanos_delta)


class Test__wait_for_operation(unittest2.TestCase):

    def _callFUT(self, cluster_connection, project_id, zone, cluster_id,
                 operation_id, timeout_seconds=None):
        from gcloud_bigtable.cluster import _wait_for_operation
        return _wait_for_operation(cluster_connection, project_id, zone,
                                   cluster_id, operation_id,
                                   timeout_seconds=timeout_seconds)

    def test_it(self):
        import time
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        op_not_done = operations_pb2.Operation(done=False)
        op_done = operations_pb2.Operation(done=True)

        connection = _MockWithAttachedMethods(op_not_done, op_not_done,
                                              op_done)
        operation_id = 101
        mock_sleep = _MockCalled()

        with _Monkey(time, sleep=mock_sleep):
            result = self._callFUT(connection, PROJECT_ID, ZONE,
                                   CLUSTER_ID, operation_id,
                                   timeout_seconds=TIMEOUT_SECONDS)

        self.assertTrue(result is op_done)
        # We expect to sleep twice since first 2 ops are not done.
        wait_time = MUT._BASE_OPERATION_WAIT_TIME
        sleep_args = [(wait_time,), (2 * wait_time,)]
        mock_sleep.check_called(self, sleep_args)
        conn_call = (
            'get_operation',
            (PROJECT_ID, ZONE, CLUSTER_ID, operation_id),
            {'timeout_seconds': TIMEOUT_SECONDS},
        )
        # We expect 3 calls since first 2 ops are not done.
        conn_called = [conn_call] * 3
        self.assertEqual(connection._called, conn_called)

    def test_timeout(self):
        import time
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        op_not_done = operations_pb2.Operation(done=False)
        results = [op_not_done] * MUT._MAX_OPERATION_WAITS

        connection = _MockWithAttachedMethods(*results)
        operation_id = 107
        mock_sleep = _MockCalled()

        with _Monkey(time, sleep=mock_sleep):
            with self.assertRaises(ValueError):
                self._callFUT(connection, PROJECT_ID, ZONE,
                              CLUSTER_ID, operation_id,
                              timeout_seconds=TIMEOUT_SECONDS)

        # We expect to sleep twice since first 2 ops are not done.
        wait_time = MUT._BASE_OPERATION_WAIT_TIME
        sleep_args = [(wait_time * 2**i,)
                      for i in xrange(MUT._MAX_OPERATION_WAITS)]
        mock_sleep.check_called(self, sleep_args)
        conn_call = (
            'get_operation',
            (PROJECT_ID, ZONE, CLUSTER_ID, operation_id),
            {'timeout_seconds': TIMEOUT_SECONDS},
        )
        # We expect 3 calls since first 2 ops are not done.
        conn_called = [conn_call] * MUT._MAX_OPERATION_WAITS
        self.assertEqual(connection._called, conn_called)


class Test__get_contents(unittest2.TestCase):

    def _callFUT(self, filename):
        from gcloud_bigtable.cluster import _get_contents
        return _get_contents(filename)

    def test_it(self):
        import tempfile

        filename = tempfile.mktemp()
        contents = b'foobar'
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)

        self.assertEqual(self._callFUT(filename), contents)
