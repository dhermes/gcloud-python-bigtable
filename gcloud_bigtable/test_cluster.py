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


PROJECT = 'project-id'
ZONE = 'zone'
CLUSTER_ID = 'cluster-id'


class Test__get_pb_property_value(unittest2.TestCase):

    def _callFUT(self, message_pb, property_name):
        from gcloud_bigtable.cluster import _get_pb_property_value
        return _get_pb_property_value(message_pb, property_name)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        serve_nodes = 119
        cluster_pb = data_pb2.Cluster(serve_nodes=serve_nodes)
        result = self._callFUT(cluster_pb, 'serve_nodes')
        self.assertEqual(result, serve_nodes)

    def test_with_value_unset_on_pb(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        cluster_pb = data_pb2.Cluster()
        with self.assertRaises(ValueError):
            self._callFUT(cluster_pb, 'serve_nodes')


class Test__prepare_create_request(unittest2.TestCase):

    def _callFUT(self, cluster):
        from gcloud_bigtable.cluster import _prepare_create_request
        return _prepare_create_request(cluster)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable.cluster import Cluster

        display_name = u'DISPLAY_NAME'
        serve_nodes = 8

        cluster = Cluster(ZONE, CLUSTER_ID, _Client(PROJECT),
                          display_name=display_name, serve_nodes=serve_nodes)
        request_pb = self._callFUT(cluster)
        self.assertTrue(isinstance(request_pb,
                                   messages_pb2.CreateClusterRequest))
        self.assertEqual(request_pb.cluster_id, CLUSTER_ID)
        self.assertEqual(request_pb.name,
                         'projects/' + PROJECT + '/zones/' + ZONE)
        self.assertTrue(isinstance(request_pb.cluster, data_pb2.Cluster))
        self.assertEqual(request_pb.cluster.display_name, display_name)
        self.assertEqual(request_pb.cluster.serve_nodes, serve_nodes)


class Test__process_operation(unittest2.TestCase):

    def _callFUT(self, operation_pb):
        from gcloud_bigtable.cluster import _process_operation
        return _process_operation(operation_pb)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        expected_operation_id = 234
        operation_name = ('operations/projects/%s/zones/%s/clusters/%s/'
                          'operations/%d' % (PROJECT, ZONE, CLUSTER_ID,
                                             expected_operation_id))

        current_op = operations_pb2.Operation(name=operation_name)

        request_metadata = messages_pb2.CreateClusterMetadata()
        mock_parse_pb_any_to_native = _MockCalled(request_metadata)
        expected_operation_begin = object()
        mock_pb_timestamp_to_datetime = _MockCalled(expected_operation_begin)
        with _Monkey(MUT, _parse_pb_any_to_native=mock_parse_pb_any_to_native,
                     _pb_timestamp_to_datetime=mock_pb_timestamp_to_datetime):
            operation_id, operation_begin = self._callFUT(current_op)

        self.assertEqual(operation_id, expected_operation_id)
        self.assertTrue(operation_begin is expected_operation_begin)

        mock_parse_pb_any_to_native.check_called(
            self, [(current_op.metadata,)])
        mock_pb_timestamp_to_datetime.check_called(
            self, [(request_metadata.request_time,)])

    def test_failure(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)

        cluster = data_pb2.Cluster()
        with self.assertRaises(ValueError):
            self._callFUT(cluster)


class TestOperation(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.cluster import Operation
        return Operation

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _constructor_test_helper(self, cluster=None):
        import datetime
        op_type = 'fake-op'
        op_id = 8915
        begin = datetime.datetime(2015, 10, 22, 1, 1)
        operation = self._makeOne(op_type, op_id, begin, cluster=cluster)

        self.assertEqual(operation.op_type, op_type)
        self.assertEqual(operation.op_id, op_id)
        self.assertEqual(operation.begin, begin)
        self.assertEqual(operation._cluster, cluster)
        self.assertFalse(operation._complete)

    def test_constructor_defaults(self):
        self._constructor_test_helper()

    def test_constructor_explicit_cluster(self):
        cluster = object()
        self._constructor_test_helper(cluster=cluster)

    def test___eq__(self):
        import datetime
        op_type = 'fake-op'
        op_id = 8915
        begin = datetime.datetime(2015, 10, 22, 1, 1)
        cluster = object()
        operation1 = self._makeOne(op_type, op_id, begin, cluster=cluster)
        operation2 = self._makeOne(op_type, op_id, begin, cluster=cluster)
        self.assertEqual(operation1, operation2)

    def test___eq__type_differ(self):
        operation1 = self._makeOne('foo', 123, None)
        operation2 = object()
        self.assertNotEqual(operation1, operation2)

    def test___ne__same_value(self):
        import datetime
        op_type = 'fake-op'
        op_id = 8915
        begin = datetime.datetime(2015, 10, 22, 1, 1)
        cluster = object()
        operation1 = self._makeOne(op_type, op_id, begin, cluster=cluster)
        operation2 = self._makeOne(op_type, op_id, begin, cluster=cluster)
        comparison_val = (operation1 != operation2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        operation1 = self._makeOne('foo', 123, None)
        operation2 = self._makeOne('bar', 456, None)
        self.assertNotEqual(operation1, operation2)

    def test_finished_without_operation(self):
        operation = self._makeOne(None, None, None)
        operation._complete = True
        with self.assertRaises(ValueError):
            operation.finished()

    def _finished_helper(self, done):
        import datetime
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable.cluster import Cluster

        project = 'PROJECT'
        zone = 'zone'
        cluster_id = 'cluster-id'
        op_type = 'fake-op'
        op_id = 789
        begin = datetime.datetime(2015, 10, 22, 1, 1)
        timeout_seconds = 1

        client = _Client(project)
        cluster = Cluster(zone, cluster_id, client)
        operation = self._makeOne(op_type, op_id, begin, cluster=cluster)

        # Create request_pb
        op_name = ('operations/projects/' + project + '/zones/' +
                   zone + '/clusters/' + cluster_id +
                   '/operations/%d' % (op_id,))
        request_pb = operations_pb2.GetOperationRequest(name=op_name)

        # Create response_pb
        response_pb = operations_pb2.Operation(done=done)

        # Patch the stub used by the API method.
        client._operations_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = done

        # Perform the method and check the result.
        result = operation.finished(timeout_seconds=timeout_seconds)

        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'GetOperation',
            (request_pb, timeout_seconds),
            {},
        )])

        if done:
            self.assertTrue(operation._complete)
        else:
            self.assertFalse(operation._complete)

    def test_finished(self):
        self._finished_helper(done=True)

    def test_finished_not_done(self):
        self._finished_helper(done=False)


class TestCluster(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.cluster import Cluster
        return Cluster

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        client = object()
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, CLUSTER_ID)
        self.assertEqual(cluster.serve_nodes, 3)
        self.assertTrue(cluster._client is client)

    def test_constructor_non_default(self):
        client = object()
        display_name = 'display_name'
        serve_nodes = 8
        cluster = self._makeOne(ZONE, CLUSTER_ID, client,
                                display_name=display_name,
                                serve_nodes=serve_nodes)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)
        self.assertEqual(cluster.display_name, display_name)
        self.assertEqual(cluster.serve_nodes, serve_nodes)
        self.assertTrue(cluster._client is client)

    def test_copy(self):
        client = _Client(PROJECT)
        display_name = 'DISPLAY_NAME'
        serve_nodes = 8
        cluster = self._makeOne(ZONE, CLUSTER_ID, client,
                                display_name=display_name,
                                serve_nodes=serve_nodes)
        new_cluster = cluster.copy()

        # Make sure the client got copied to a new instance.
        self.assertFalse(new_cluster._client is client)
        self.assertEqual(new_cluster._client.__dict__,
                         client.__dict__)
        # Just replace the client on the new_cluster so we can
        # check cluster equality.
        new_cluster._client = client
        self.assertFalse(cluster is new_cluster)
        self.assertEqual(cluster, new_cluster)

    def test_client_getter(self):
        client = object()
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertTrue(cluster.client is client)

    def test_project_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(project=PROJECT, credentials=credentials)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.project, PROJECT)

    def test_timeout_seconds_getter(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        timeout_seconds = 77
        client = Client(project=PROJECT, credentials=credentials,
                        timeout_seconds=timeout_seconds)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        self.assertEqual(cluster.timeout_seconds, timeout_seconds)

    def test_name_property(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(project=PROJECT, credentials=credentials)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)
        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        self.assertEqual(cluster.name, cluster_name)

    def test_table_factory(self):
        from gcloud_bigtable.table import Table

        cluster = self._makeOne(ZONE, CLUSTER_ID, None)
        table_id = 'table_id'
        table = cluster.table(table_id)
        self.assertTrue(isinstance(table, Table))
        self.assertEqual(table.table_id, table_id)
        self.assertEqual(table._cluster, cluster)

    def test_from_pb_success(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        client = Client(project=PROJECT, credentials=credentials)

        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster_pb = data_pb2.Cluster(
            name=cluster_name,
            display_name=CLUSTER_ID,
            serve_nodes=3,
        )

        klass = self._getTargetClass()
        cluster = klass.from_pb(cluster_pb, client)
        self.assertTrue(isinstance(cluster, klass))
        self.assertEqual(cluster.client, client)
        self.assertEqual(cluster.zone, ZONE)
        self.assertEqual(cluster.cluster_id, CLUSTER_ID)

    def test_from_pb_bad_cluster_name(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)

        cluster_name = 'INCORRECT_FORMAT'
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, None)

    def test_from_pb_project_mistmatch(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        alt_project = 'ALT_PROJECT'
        client = Client(project=alt_project, credentials=credentials)

        self.assertNotEqual(PROJECT, alt_project)

        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        cluster_pb = data_pb2.Cluster(name=cluster_name)

        klass = self._getTargetClass()
        with self.assertRaises(ValueError):
            klass.from_pb(cluster_pb, client)

    def test___eq__(self):
        zone = 'zone'
        cluster_id = 'cluster_id'
        client = object()
        cluster1 = self._makeOne(zone, cluster_id, client)
        cluster2 = self._makeOne(zone, cluster_id, client)
        self.assertEqual(cluster1, cluster2)

    def test___eq__type_differ(self):
        cluster1 = self._makeOne('zone', 'cluster_id', 'client')
        cluster2 = object()
        self.assertNotEqual(cluster1, cluster2)

    def test___ne__same_value(self):
        zone = 'zone'
        cluster_id = 'cluster_id'
        client = object()
        cluster1 = self._makeOne(zone, cluster_id, client)
        cluster2 = self._makeOne(zone, cluster_id, client)
        comparison_val = (cluster1 != cluster2)
        self.assertFalse(comparison_val)

    def test___ne__(self):
        cluster1 = self._makeOne('zone1', 'cluster_id1', 'client1')
        cluster2 = self._makeOne('zone2', 'cluster_id2', 'client2')
        self.assertNotEqual(cluster1, cluster2)

    def test_reload(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client(PROJECT)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)

        # Create request_pb
        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.GetClusterRequest(name=cluster_name)

        # Create response_pb
        response_pb = data_pb2.Cluster(
            display_name=CLUSTER_ID,
            serve_nodes=3,
        )

        # Patch the stub used by the API method.
        client._cluster_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = None  # reload() has no return value.

        # Perform the method and check the result.
        timeout_seconds = 123
        result = cluster.reload(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'GetCluster',
            (request_pb, timeout_seconds),
            {},
        )])

    def test_create(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        client = _Client(PROJECT)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)

        # Create request_pb. Just a mock since we monkey patch
        # _prepare_create_request
        request_pb = object()

        # Create response_pb
        current_op = operations_pb2.Operation()
        response_pb = data_pb2.Cluster(current_operation=current_op)

        # Patch the stub used by the API method.
        client._cluster_stub = stub = StubMock(response_pb)

        # Create expected_result.
        op_id = 5678
        op_begin = object()
        expected_result = MUT.Operation('create', op_id, op_begin,
                                        cluster=cluster)

        # Perform the method and check the result.
        timeout_seconds = 578
        mock_prepare_create_request = _MockCalled(request_pb)
        mock_process_operation = _MockCalled((op_id, op_begin))
        with _Monkey(MUT, _prepare_create_request=mock_prepare_create_request,
                     _process_operation=mock_process_operation):
            result = cluster.create(timeout_seconds=timeout_seconds)

        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'CreateCluster',
            (request_pb, timeout_seconds),
            {},
        )])
        mock_prepare_create_request.check_called(self, [(cluster,)])
        mock_process_operation.check_called(self, [(current_op,)])

    def test_update(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        client = _Client(PROJECT)
        serve_nodes = 81
        display_name = 'display_name'
        cluster = self._makeOne(ZONE, CLUSTER_ID, client,
                                display_name=display_name,
                                serve_nodes=serve_nodes)

        # Create request_pb
        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = data_pb2.Cluster(
            name=cluster_name,
            display_name=display_name,
            serve_nodes=serve_nodes,
        )

        # Create response_pb
        current_op = operations_pb2.Operation()
        response_pb = data_pb2.Cluster(current_operation=current_op)

        # Patch the stub used by the API method.
        client._cluster_stub = stub = StubMock(response_pb)

        # Create expected_result.
        op_id = 5678
        op_begin = object()
        expected_result = MUT.Operation('update', op_id, op_begin,
                                        cluster=cluster)

        # We must create the cluster object with the client passed in.
        timeout_seconds = 9
        mock_process_operation = _MockCalled((op_id, op_begin))
        with _Monkey(MUT,
                     _process_operation=mock_process_operation):
            result = cluster.update(timeout_seconds=timeout_seconds)

        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'UpdateCluster',
            (request_pb, timeout_seconds),
            {},
        )])
        mock_process_operation.check_called(self, [(current_op,)])

    def test_delete(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client(PROJECT)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)

        # Create request_pb
        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.DeleteClusterRequest(name=cluster_name)

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        client._cluster_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = None  # delete() has no return value.

        # Perform the method and check the result.
        timeout_seconds = 57
        result = cluster.delete(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'DeleteCluster',
            (request_pb, timeout_seconds),
            {},
        )])

    def test_undelete(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import operations_pb2
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import cluster as MUT

        client = _Client(PROJECT)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)

        # Create request_pb
        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = messages_pb2.UndeleteClusterRequest(name=cluster_name)

        # Create response_pb
        response_pb = operations_pb2.Operation()

        # Patch the stub used by the API method.
        client._cluster_stub = stub = StubMock(response_pb)

        # Create expected_result.
        op_id = 5678
        op_begin = object()
        expected_result = MUT.Operation('undelete', op_id, op_begin,
                                        cluster=cluster)

        # Perform the method and check the result.
        timeout_seconds = 78
        mock_process_operation = _MockCalled((op_id, op_begin))
        with _Monkey(MUT,
                     _process_operation=mock_process_operation):
            result = cluster.undelete(timeout_seconds=timeout_seconds)

        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'UndeleteCluster',
            (request_pb, timeout_seconds),
            {},
        )])
        mock_process_operation.check_called(self, [(response_pb,)])

    def _list_tables_helper(self, table_id, table_name=None):
        from gcloud_bigtable._generated import (
            bigtable_table_data_pb2 as table_data_pb2)
        from gcloud_bigtable._generated import (
            bigtable_table_service_messages_pb2 as table_messages_pb2)
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client(PROJECT)
        cluster = self._makeOne(ZONE, CLUSTER_ID, client)

        # Create request_
        cluster_name = ('projects/' + PROJECT + '/zones/' + ZONE +
                        '/clusters/' + CLUSTER_ID)
        request_pb = table_messages_pb2.ListTablesRequest(name=cluster_name)

        # Create response_pb
        table_name = table_name or (cluster_name + '/tables/' + table_id)
        response_pb = table_messages_pb2.ListTablesResponse(
            tables=[
                table_data_pb2.Table(name=table_name),
            ],
        )

        # Patch the stub used by the API method.
        client._table_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_table = cluster.table(table_id)
        expected_result = [expected_table]

        # Perform the method and check the result.
        timeout_seconds = 45
        result = cluster.list_tables(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'ListTables',
            (request_pb, timeout_seconds),
            {},
        )])

    def test_list_tables(self):
        table_id = 'table_id'
        self._list_tables_helper(table_id)

    def test_list_tables_failure_bad_split(self):
        with self.assertRaises(ValueError):
            self._list_tables_helper(None, table_name='wrong-format')

    def test_list_tables_failure_name_bad_before(self):
        table_id = 'table_id'
        bad_table_name = ('nonempty-section-before' +
                          'projects/' + PROJECT + '/zones/' + ZONE +
                          '/clusters/' + CLUSTER_ID + '/tables/' + table_id)
        with self.assertRaises(ValueError):
            self._list_tables_helper(table_id, table_name=bad_table_name)


class _Client(object):

    cluster_stub = None
    operations_stub = None
    table_stub = None

    def __init__(self, project):
        self.project = project
        self.project_name = 'projects/' + project

    def copy(self):
        import copy as copy_module
        return copy_module.deepcopy(self)
