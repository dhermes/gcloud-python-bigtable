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

"""Connection to Google Cloud BigTable API servers."""

import httplib2


DATA_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.data'
READ_ONLY_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.data.readonly'
ADMIN_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'

DATA_API_BASE_URL = 'https://bigtable.googleapis.com'
TABLE_ADMIN_API_BASE_URL = 'https://bigtabletableadmin.googleapis.com'
CLUSTER_ADMIN_API_BASE_URL = 'https://bigtableclusteradmin.googleapis.com'


class Connection(object):
    """HTTP-RPC Connection base class for Google Cloud BigTable.

    If no value is passed in for ``http``, a :class:`httplib2.Http` object
    will be created and authorized with the ``credentials``. If not, the
    ``credentials`` and ``http`` need not be related.

    A custom (non-``httplib2``) HTTP object must have a ``request`` method
    which accepts the following arguments:

    * ``uri``
    * ``method``
    * ``body``
    * ``headers``

    In addition, ``redirections`` and ``connection_type`` may be used.

    Without the use of ``credentials.authorize(http)``, a custom ``http``
    object will also need to be able to add a bearer token to API
    requests and handle token refresh on 401 errors.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                       :class:`NoneType`
    :param credentials: The OAuth2 Credentials to use for this connection.

    :type http: :class:`httplib2.Http` or class that defines ``request()``.
    :param http: An optional HTTP object to make requests.
    """

    USER_AGENT = 'gcloud-bigtable-python'

    def __init__(self, credentials=None, http=None):
        self._http = http
        self._credentials = credentials

    @property
    def credentials(self):
        """Getter for current credentials.

        :rtype: :class:`oauth2client.client.OAuth2Credentials` or
                :class:`NoneType`
        :returns: The credentials object associated with this connection.
        """
        return self._credentials

    @property
    def http(self):
        """A getter for the HTTP transport used in talking to the API.

        :rtype: :class:`httplib2.Http`
        :returns: A Http object used to transport data.
        """
        if self._http is None:
            self._http = httplib2.Http()
            if self._credentials:
                self._http = self._credentials.authorize(self._http)
        return self._http

    @staticmethod
    def _create_scoped_credentials(credentials, scope):
        """Create a scoped set of credentials if it is required.

        :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                           :class:`NoneType`
        :param credentials: The OAuth2 Credentials to add a scope to.

        :type scope: list of URLs
        :param scope: the effective service auth scopes for the connection.

        :rtype: :class:`oauth2client.client.OAuth2Credentials` or
                :class:`NoneType`
        :returns: A new credentials object that has a scope added (if needed).
        """
        if credentials and credentials.create_scoped_required():
            credentials = credentials.create_scoped(scope)
        return credentials

    def _request(self, request_uri, data, request_method='POST'):
        """Make a request over the Http transport to the Cloud Datastore API.

        :type request_uri: string
        :param request_uri: The URI to send the request to.

        :type data: string
        :param data: The data to send with the API call.
                     Typically this is a serialized Protobuf string.

        :type request_method: string
        :param request_method: The HTTP method to send the request. Defaults to
                               'POST'.

        :rtype: string
        :returns: The string response content from the API call.
        :raises: :class:`ValueError` if the response code is not 200 OK.
        """
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Content-Length': str(len(data)),
            'User-Agent': self.USER_AGENT,
        }
        headers, content = self.http.request(
            uri=request_uri, method=request_method,
            headers=headers, body=data)

        status = headers['status']
        if status != '200':
            raise ValueError(headers, content)

        return content

    def _rpc(self, request_uri, request_pb, response_pb_cls,
             request_method='POST'):
        """Make a protobuf RPC request.

        :type request_uri: string
        :param request_uri: The URI to send the request to.

        :type request_pb: :class:`google.protobuf.message.Message` instance
        :param request_pb: the protobuf instance representing the request.

        :type response_pb_cls: A :class:`google.protobuf.message.Message'
                               subclass.
        :param response_pb_cls: The class used to unmarshall the response
                                protobuf.

        :type request_method: string
        :param request_method: The HTTP method to send the request. Defaults to
                               'POST'.
        """
        response = self._request(request_uri=request_uri,
                                 data=request_pb.SerializeToString(),
                                 request_method=request_method)
        return response_pb_cls.FromString(response)


class DataConnection(Connection):
    """Connection to Google Cloud BigTable Data API.

    This only allows interacting with data in an existing table.

    The ``table_name`` value must take the form:
        "projects/*/zones/*/clusters/*/tables/*"
    """

    API_VERSION = 'v1'
    """The version of the API, used in building the API call's URL."""

    API_URL_TEMPLATE = ('{api_base}/{api_version}/{table_name}/'
                        'rows{final_segment}')
    """A template for the URL of a particular API call."""

    def __init__(self, credentials=None, http=None):
        credentials = self._create_scoped_credentials(
            credentials, (DATA_SCOPE,))
        super(DataConnection, self).__init__(credentials=credentials, http=http)

    def build_api_url(self, table_name, rpc_method, row_key=None):
        """Construct the URL for a particular API call.

        This method is used internally to come up with the URL to use when
        making RPCs to the Google Cloud BigTable API.

        :type table_name: string
        :param table_name: The name of a table. Expected to be of the form
                           "projects/*/zones/*/clusters/*/tables/*".

        :type rpc_method: string
        :param rpc_method: The RPC method name for the URL.

        :type row_key: string or ``NoneType``
        :param row_key: (Optional). The row key for the RPC operation.

        :rtype: string
        :returns: The URL needed to make an API request.
        """
        if row_key is None:
            final_segment = ':' + rpc_method
        else:
            final_segment = '/' + row_key + ':' + rpc_method

        return self.API_URL_TEMPLATE.format(
            api_base=DATA_API_BASE_URL,
            api_version=self.API_VERSION,
            table_name=table_name,
            final_segment=final_segment)

    def read_rows(self, table_name):
        request_uri = self.build_api_url(table_name, 'read')
        raise NotImplementedError

    def sample_row_keys(self, table_name):
        request_uri = self.build_api_url(table_name, 'sampleKeys')
        request_method = 'GET'
        raise NotImplementedError

    def mutate_row(self, table_name, row_key):
        request_uri = self.build_api_url(table_name, 'mutate',
                                         row_key=row_key)
        raise NotImplementedError

    def check_and_mutate_row(self, table_name, row_key):
        request_uri = self.build_api_url(table_name, 'checkAndMutate',
                                         row_key=row_key)
        raise NotImplementedError

    def read_modify_write_row(self, table_name, row_key):
        request_uri = self.build_api_url(table_name, 'readModifyWrite',
                                         row_key=row_key)
        raise NotImplementedError
