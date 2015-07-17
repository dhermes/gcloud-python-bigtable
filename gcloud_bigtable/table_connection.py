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

"""Connection to Google Cloud BigTable Table Admin API."""

from gcloud_bigtable._generated import bigtable_table_service_messages_pb2
from gcloud_bigtable.connection import Connection


class TableConnection(Connection):
    """Connection to Google Cloud BigTable Table API.

    This only allows interacting with tables in a cluster.

    The ``cluster_name`` value must take the form:
        "projects/*/zones/*/clusters/*"
    """

    API_VERSION = 'v1'
    """The version of the API, used in building the API call's URL."""

    API_URL_TEMPLATE = ('{api_base}/{api_version}/{cluster_name}/'
                        'tables{final_segment}')
    """A template for the URL of a particular API call."""

    API_BASE_URL = 'https://bigtabletableadmin.googleapis.com'
    """Base URL for API requests."""

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
    """Scope for table and cluster API requests."""

    @classmethod
    def build_api_url(cls, cluster_name, table_name=None, table_method=None,
                      column_family=None):
        """Construct the URL for a particular API call.

        This method is used internally to come up with the URL to use when
        making RPCs to the Google Cloud BigTable Table API.

        :type cluster_name: string
        :param cluster_name: (Optional) The name of a cluster.

        :type table_name: string
        :param table_name: (Optional) The name of a table.

        :type table_method: string
        :param table_method: (Optional) The method for a table. May either be
                             of the form ":{rpc_verb}" or "/pathToMethod".

        :type column_family: string
        :param column_family: (Optional) A column family to create.

        :rtype: string
        :returns: The URL needed to make an API request.
        :raises: :class:`ValueError` If ``table_method`` is set with
                 ``column_family`` or without ``table_method``, or if
                 ``column_family`` is set without ``table_name``.
        """
        if table_name is None:
            final_segment = ''
        else:
            final_segment = '/' + table_name

        if table_method is not None:
            if table_name is None:
                raise ValueError('table_name must be set if table_method is')
            if column_family is not None:
                raise ValueError('column_family cannot be set if '
                                 'table_method is')
            final_segment += table_method

        if column_family is not None:
            if table_name is None:
                raise ValueError('table_name must be set if column_family is')
            final_segment += '/columnFamilies/' + column_family

        return cls.API_URL_TEMPLATE.format(
            api_base=cls.API_BASE_URL,
            api_version=cls.API_VERSION,
            cluster_name=cluster_name,
            final_segment=final_segment)

    def create_table(self, cluster_name):
        request_uri = self.build_api_url(cluster_name)
        request_method = 'POST'
        raise NotImplementedError

    def list_tables(self, cluster_name):
        request_uri = self.build_api_url(cluster_name)
        request_method = 'GET'
        raise NotImplementedError

    def get_table(self, cluster_name, table_name):
        request_uri = self.build_api_url(cluster_name, table_name=table_name)
        request_method = 'GET'
        raise NotImplementedError

    def delete_table(self, cluster_name, table_name):
        request_uri = self.build_api_url(cluster_name, table_name=table_name)
        request_method = 'DELETE'
        raise NotImplementedError

    def rename_table(self, cluster_name, table_name):
        request_uri = self.build_api_url(cluster_name, table_name=table_name,
                                         table_method=':rename')
        request_method = 'POST'
        raise NotImplementedError

    def create_column_family(self, cluster_name, table_name):
        request_uri = self.build_api_url(cluster_name, table_name=table_name,
                                         table_method='/columnFamilies')
        request_method = 'POST'
        raise NotImplementedError

    def update_column_family(self, cluster_name, table_name, column_family):
        request_uri = self.build_api_url(cluster_name, table_name=table_name,
                                         column_family=column_family)
        request_method = 'PUT'
        raise NotImplementedError

    def delete_column_family(self, cluster_name, table_name, column_family):
        request_uri = self.build_api_url(cluster_name, table_name=table_name,
                                         column_family=column_family)
        request_method = 'DELETE'
        raise NotImplementedError
