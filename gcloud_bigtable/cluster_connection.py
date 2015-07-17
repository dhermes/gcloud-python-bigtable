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

"""Connection to Google Cloud BigTable Cluster Admin API."""

from gcloud_bigtable._generated import bigtable_cluster_service_messages_pb2
from gcloud_bigtable.connection import Connection


class ClusterConnection(Connection):
    """Connection to Google Cloud BigTable Cluster API.

    This only allows interacting with clusters in a project.
    """

    API_VERSION = 'v1'
    """The version of the API, used in building the API call's URL."""

    API_URL_TEMPLATE = ('{api_base}/{api_version}/projects/{project_name}/'
                        '{final_segment}')
    """A template for the URL of a particular API call."""

    API_BASE_URL = 'https://bigtableclusteradmin.googleapis.com'
    """Base URL for API requests."""

    SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
    """Scope for table and cluster API requests."""

    @classmethod
    def build_api_url(cls, project_name, aggregated=False, zone_name=None,
                      cluster_name=None, undelete=False):
        """Construct the URL for a particular API call.

        This method is used internally to come up with the URL to use when
        making RPCs to the Google Cloud BigTable Cluster API.

        :type project_name: string
        :param project_name: The name of the project that owns the clusters.

        :type aggregated: boolean
        :param aggregated: (Optional) Whether the URI should be aggregated
                           (for :meth:`list_clusters`). Defaults to ``False``.
                           If ``True``, no other keyword arguments can be set.

        :type zone_name: string
        :param zone_name: (Optional) The name of the zone in the request.

        :type cluster_name: string
        :param cluser_name: (Optional) The name of the cluster in the request.
                            If included, ``zone_name`` is also required.

        :type undelete: boolean
        :param undelete: (Optional) Whether the URI should be an ``undelete``
                         required (for :meth:`undelete_cluster`). Defaults
                         to ``False``. If ``True``, both ``cluster_name`` and
                         ``zone_name`` are required.

        :rtype: string
        :returns: The URL needed to make an API request.
        :raises: :class:`ValueError` If ``aggregated`` is set with
                 ``zone_name``, or if ``cluster_name`` is set without
                 ``zone_name`` or if ``undelete`` is set without
                 ``cluster_name``.
        """
        if aggregated:
            final_segment = 'aggregated/clusters'
        else:
            final_segment = 'zones'

        if zone_name is not None:
            if aggregated:
                raise ValueError('aggregated cannot be set if zone_name is')
            final_segment += '/' + zone_name + '/clusters'

        if cluster_name is not None:
            if zone_name is None:
                # NOTE: This also covers conflicts from aggregated, since it
                #       is checked above.
                raise ValueError('zone_name must be set if cluster_name is')

            final_segment += '/' + cluster_name

        if undelete:
            if cluster_name is None:
                # NOTE: This also covers conflicts from aggregated and a missing
                #       zone_name, since they are checked above.
                raise ValueError('zone_name and cluster_name must be set '
                                 'if undelete is')
            final_segment += ':undelete'

        return cls.API_URL_TEMPLATE.format(
            api_base=cls.API_BASE_URL,
            api_version=cls.API_VERSION,
            project_name=project_name,
            final_segment=final_segment)

    def list_zones(self, project_name):
        request_uri = self.build_api_url(project_name)
        response_class = bigtable_cluster_service_messages_pb2.ListZonesResponse
        request_pb = bigtable_cluster_service_messages_pb2.ListZonesRequest()
        response = self._rpc(request_uri, request_pb, response_class,
                             request_method='GET')
        return response

    def get_cluster(self, project_name, zone_name, cluster_name):
        request_uri = self.build_api_url(project_name, zone_name=zone_name,
                                         cluster_name=cluster_name)
        request_method = 'GET'
        raise NotImplementedError

    def list_clusters(self, project_name):
        request_uri = self.build_api_url(project_name, aggregated=True)
        request_method = 'GET'
        raise NotImplementedError

    def create_cluster(self, project_name, zone_name):
        request_uri = self.build_api_url(project_name, zone_name=zone_name)
        request_method = 'POST'
        raise NotImplementedError

    def update_cluster(self, project_name, zone_name, cluster_name):
        request_uri = self.build_api_url(project_name, zone_name=zone_name,
                                         cluster_name=cluster_name)
        request_method = 'PUT'
        raise NotImplementedError

    def delete_cluster(self, project_name, zone_name, cluster_name):
        request_uri = self.build_api_url(project_name, zone_name=zone_name,
                                         cluster_name=cluster_name)
        request_method = 'DELETE'
        raise NotImplementedError

    def undelete_cluster(self, project_name, zone_name, cluster_name):
        request_uri = self.build_api_url(project_name, zone_name=zone_name,
                                         cluster_name=cluster_name,
                                         undelete=True)
        request_method = 'POST'
        raise NotImplementedError
