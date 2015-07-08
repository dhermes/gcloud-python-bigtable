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
        from gcloud_bigtable.connection import Connection
        return Connection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (None,))
        self.assertTrue(connection._http is None)

    def test__create_scoped_credentials_with_no_credentials(self):
        klass = self._getTargetClass()
        credentials = None
        scope = object()

        new_credentials = klass._create_scoped_credentials(credentials, scope)
        self.assertTrue(new_credentials is credentials)

    def test__create_scoped_credentials_with_credentials(self):
        klass = self._getTargetClass()
        credentials = _Credentials()
        scope = object()

        new_credentials = klass._create_scoped_credentials(credentials, scope)
        self.assertTrue(new_credentials is credentials)
        self.assertTrue(credentials._scopes is scope)

    def test_credentials_property(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection.credentials is credentials)

    def test_http_property_no_credentials(self):
        import httplib2

        connection = self._makeOne()
        self.assertTrue(connection._credentials is None)
        http = connection.http
        self.assertTrue(isinstance(http, httplib2.Http))
        # Make sure no credentials have been bound to the request.
        self.assertTrue(getattr(http.request, 'credentials', None) is None)

    def test_http_property(self):
        class _CustomCredentials(_Credentials):

            _authorized = None

            def authorize(self, http_obj):
                self._authorized = http_obj
                return http_obj

        credentials = _CustomCredentials()
        connection = self._makeOne(credentials=credentials)
        http = connection.http
        self.assertTrue(http is credentials._authorized)

    def _request_test_helper(self, status):
        headers = {'status': status}
        content = b'FOOBAR'
        http = _Http(headers, content)
        connection = self._makeOne(http=http)
        request_uri = 'https://domain.com/path'
        data = b'DEADBEEF'
        request_method = 'PUT'
        if status == '200':
            result = connection._request(request_uri, data,
                                         request_method=request_method)
            self.assertEqual(result, content)
        else:
            self.assertRaises(ValueError, connection._request, request_uri,
                              data, request_method=request_method)

        self.assertEqual(len(http._called_with), 4)
        self.assertEqual(http._called_with['method'], request_method)
        self.assertEqual(http._called_with['uri'], request_uri)
        self.assertEqual(http._called_with['body'], data)
        expected_headers = {
            'Content-Length': str(len(data)),
            'Content-Type': 'application/x-protobuf',
            'User-Agent': 'gcloud-bigtable-python',
        }
        self.assertEqual(http._called_with['headers'], expected_headers)

    def test__request_success(self):
        self._request_test_helper('200')

    def test__request_failure(self):
        self._request_test_helper('418')

    def test__rpc(self):
        klass = self._getTargetClass()
        RESPONSE = object()
        DATA = object()
        REQUEST_URI = object()
        REQUEST_METHOD = object()

        class _MockRequestConnection(klass):

            def __init__(self):
                self._request_uris = []
                self._data = []
                self._request_methods = []

            def _request(self, request_uri, data, request_method='POST'):
                self._request_uris.append(request_uri)
                self._data.append(data)
                self._request_methods.append(request_method)
                return RESPONSE

        class _MockRequestPb(object):

            @staticmethod
            def SerializeToString():
                return DATA

        class _MockResponsePbClass(object):

            _from_string_called = []

            @classmethod
            def FromString(cls, value):
                cls._from_string_called.append(value)
                return value

        connection = _MockRequestConnection()
        result = connection._rpc(REQUEST_URI, _MockRequestPb,
                                 _MockResponsePbClass,
                                 request_method=REQUEST_METHOD)
        self.assertTrue(result is RESPONSE)
        self.assertEqual(connection._request_uris, [REQUEST_URI])
        self.assertEqual(connection._data, [DATA])
        self.assertEqual(connection._request_methods, [REQUEST_METHOD])
        self.assertEqual(_MockResponsePbClass._from_string_called, [RESPONSE])


class TestDataConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.connection import DataConnection
        return DataConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))
        self.assertTrue(connection._http is None)

    def test_build_api_url(self):
        klass = self._getTargetClass()
        table_name = 'table_name'
        rpc_method = 'rpc_method'
        api_url = klass.build_api_url(table_name, rpc_method)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            table_name,
            'rows:' + rpc_method,
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_with_row_key(self):
        klass = self._getTargetClass()
        table_name = 'table_name'
        rpc_method = 'rpc_method'
        row_key = 'row_key'
        api_url = klass.build_api_url(table_name, rpc_method, row_key=row_key)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            table_name,
            'rows',
            row_key + ':' + rpc_method,
        ])
        self.assertEqual(api_url, expected_url)

    def test_read_rows(self):
        self.assertTrue(False)

    def test_sample_row_keys(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        self.assertRaises(NotImplementedError, connection.sample_row_keys,
                          table_name)

    def test_mutate_row(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError, connection.mutate_row,
                          table_name, row_key)

    def test_check_and_mutate_row(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError, connection.check_and_mutate_row,
                          table_name, row_key)

    def test_read_modify_write_row(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        table_name = object()
        row_key = 'row_key'
        self.assertRaises(NotImplementedError,
                          connection.read_modify_write_row,
                          table_name, row_key)


class TestTableConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.connection import TableConnection
        return TableConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))
        self.assertTrue(connection._http is None)

    def test_build_api_url(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        api_url = klass.build_api_url(cluster_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            cluster_name,
            'tables',
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_table_method_missing_table_name(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        table_method = 'table_method'
        self.assertRaises(ValueError, klass.build_api_url, cluster_name,
                          table_method=table_method)

    def test_build_api_url_table_method_conflict(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        table_method = 'table_method'
        table_name = 'table_name'
        column_family = 'column_family'
        self.assertRaises(ValueError, klass.build_api_url, cluster_name,
                          table_method=table_method,
                          table_name=table_name, column_family=column_family)

    def test_build_api_url_table_method_success(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        table_method = 'table_method'
        table_name = 'table_name'
        api_url = klass.build_api_url(cluster_name, table_method=table_method,
                                      table_name=table_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            cluster_name,
            'tables',
            table_name + table_method,
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_column_family_missing_table_name(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        column_family = 'column_family'
        self.assertRaises(ValueError, klass.build_api_url, cluster_name,
                          column_family=column_family)

    def test_build_api_url_column_family_success(self):
        klass = self._getTargetClass()

        cluster_name = 'cluster_name'
        column_family = 'column_family'
        table_name = 'table_name'
        api_url = klass.build_api_url(
            cluster_name, column_family=column_family, table_name=table_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            cluster_name,
            'tables',
            table_name,
            'columnFamilies',
            column_family,
        ])
        self.assertEqual(api_url, expected_url)

    def test_create_table(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        self.assertRaises(NotImplementedError, connection.create_table,
                          cluster_name)

    def test_list_tables(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        self.assertRaises(NotImplementedError, connection.list_tables,
                          cluster_name)

    def test_get_table(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.get_table,
                          cluster_name, table_name)

    def test_delete_table(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.delete_table,
                          cluster_name, table_name)

    def test_rename_table(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.rename_table,
                          cluster_name, table_name)

    def test_create_column_family(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        self.assertRaises(NotImplementedError, connection.create_column_family,
                          cluster_name, table_name)

    def test_update_column_family(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        column_family = 'column_family'
        self.assertRaises(NotImplementedError, connection.update_column_family,
                          cluster_name, table_name, column_family)

    def test_delete_column_family(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        cluster_name = object()
        table_name = 'table_name'
        column_family = 'column_family'
        self.assertRaises(NotImplementedError, connection.delete_column_family,
                          cluster_name, table_name, column_family)


class TestClusterConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.connection import ClusterConnection
        return ClusterConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))
        self.assertTrue(connection._http is None)

    def test_build_api_url(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        api_url = klass.build_api_url(project_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            'projects',
            project_name,
            'zones',
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_aggregated(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        api_url = klass.build_api_url(project_name, aggregated=True)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            'projects',
            project_name,
            'aggregated',
            'clusters',
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_zone_name(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        zone_name = 'zone_name'
        api_url = klass.build_api_url(project_name, zone_name=zone_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            'projects',
            project_name,
            'zones',
            zone_name,
            'clusters',
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_zone_name_conflict(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        zone_name = 'zone_name'
        self.assertRaises(ValueError, klass.build_api_url, project_name,
                          zone_name=zone_name, aggregated=True)

    def test_build_api_url_cluster_name(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        api_url = klass.build_api_url(project_name, zone_name=zone_name,
                                      cluster_name=cluster_name)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            'projects',
            project_name,
            'zones',
            zone_name,
            'clusters',
            cluster_name,
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_cluster_name_missing_zone_name(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        cluster_name = 'cluster_name'
        self.assertRaises(ValueError, klass.build_api_url, project_name,
                          cluster_name=cluster_name)

    def test_build_api_url_undelete(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        api_url = klass.build_api_url(project_name, zone_name=zone_name,
                                      cluster_name=cluster_name, undelete=True)

        expected_url = '/'.join([
            klass.API_BASE_URL,
            klass.API_VERSION,
            'projects',
            project_name,
            'zones',
            zone_name,
            'clusters',
            cluster_name + ':undelete',
        ])
        self.assertEqual(api_url, expected_url)

    def test_build_api_url_undelete_missing_zone_name(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        cluster_name = 'cluster_name'
        self.assertRaises(ValueError, klass.build_api_url, project_name,
                          cluster_name=cluster_name, undelete=True)

    def test_build_api_url_undelete_missing_cluster_name(self):
        klass = self._getTargetClass()

        project_name = 'project_name'
        zone_name = 'zone_name'
        self.assertRaises(ValueError, klass.build_api_url, project_name,
                          zone_name=zone_name, undelete=True)

    def test_list_zones(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        self.assertRaises(NotImplementedError, connection.list_zones,
                          project_name)

    def test_get_cluster(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.get_cluster,
                          project_name, zone_name, cluster_name)

    def test_list_clusters(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        self.assertRaises(NotImplementedError, connection.list_clusters,
                          project_name)

    def test_create_cluster(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        self.assertRaises(NotImplementedError, connection.create_cluster,
                          project_name, zone_name)

    def test_update_cluster(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.update_cluster,
                          project_name, zone_name, cluster_name)

    def test_delete_cluster(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.delete_cluster,
                          project_name, zone_name, cluster_name)

    def test_undelete_cluster(self):
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.undelete_cluster,
                          project_name, zone_name, cluster_name)


class _Credentials(object):

    _scopes = None

    @staticmethod
    def create_scoped_required():
        return True

    def create_scoped(self, scope):
        self._scopes = scope
        return self


class _Http(object):

    _called_with = None

    def __init__(self, headers, content):
        from httplib2 import Response
        self._response = Response(headers)
        self._content = content

    def request(self, **kw):
        self._called_with = kw
        return self._response, self._content
