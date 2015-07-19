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


class Test_make_cluster_stub(unittest2.TestCase):

    def _callFUT(self, credentials):
        from gcloud_bigtable.cluster_connection import make_cluster_stub
        return make_cluster_stub(credentials)

    def test_it(self):
        from gcloud_bigtable._testing import _Credentials
        from gcloud_bigtable import cluster_connection as MUT

        called = []
        creds_list = []
        mock_result = object()
        transformed = object()

        def custom_factory(*args, **kwargs):
            called.append((args, kwargs))
            return mock_result

        def transformer(credentials):
            creds_list.append(credentials)
            return transformed

        certs = 'FOOBAR'
        credentials = _Credentials()
        orig_factory = MUT.CLUSTER_STUB_FACTORY
        orig_get_certs = MUT.get_certs
        orig_MetadataTransformer = MUT.MetadataTransformer
        try:
            MUT.CLUSTER_STUB_FACTORY = custom_factory
            MUT.get_certs = lambda: certs
            MUT.MetadataTransformer = transformer
            result = self._callFUT(credentials)
        finally:
            MUT.CLUSTER_STUB_FACTORY = orig_factory
            MUT.get_certs = orig_get_certs
            MUT.MetadataTransformer = orig_MetadataTransformer

        self.assertTrue(result is mock_result)
        self.assertEqual(creds_list, [credentials])
        # Unpack single call.
        (called_args, called_kwargs), = called
        self.assertEqual(called_args,
                         (MUT.CLUSTER_ADMIN_HOST, MUT.PORT))
        expected_kwargs = {
            'metadata_transformer': transformed,
            'secure': True,
            'root_certificates': certs,
        }
        self.assertEqual(called_kwargs, expected_kwargs)


class TestClusterConnection(unittest2.TestCase):

    @staticmethod
    def _getTargetClass():
        from gcloud_bigtable.cluster_connection import ClusterConnection
        return ClusterConnection

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _Credentials
        klass = self._getTargetClass()
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)
        self.assertTrue(connection._credentials is credentials)
        self.assertEqual(connection._credentials._scopes, (klass.SCOPE,))

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
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        self.assertRaises(NotImplementedError, connection.list_zones,
                          project_name)

    def test_get_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.get_cluster,
                          project_name, zone_name, cluster_name)

    def test_list_clusters(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        self.assertRaises(NotImplementedError, connection.list_clusters,
                          project_name)

    def test_create_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        self.assertRaises(NotImplementedError, connection.create_cluster,
                          project_name, zone_name)

    def test_update_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.update_cluster,
                          project_name, zone_name, cluster_name)

    def test_delete_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.delete_cluster,
                          project_name, zone_name, cluster_name)

    def test_undelete_cluster(self):
        from gcloud_bigtable._testing import _Credentials
        credentials = _Credentials()
        connection = self._makeOne(credentials=credentials)

        project_name = object()
        zone_name = 'zone_name'
        cluster_name = 'cluster_name'
        self.assertRaises(NotImplementedError, connection.undelete_cluster,
                          project_name, zone_name, cluster_name)
