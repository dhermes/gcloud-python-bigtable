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

"""Connection to Google Cloud Bigtable API servers."""


from gcloud_bigtable._helpers import get_certs


TIMEOUT_SECONDS = 10


class MetadataTransformer(object):
    """Callable class to transform metadata for gRPC requests.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials`
    :param credentials: The OAuth2 Credentials to use for access tokens
                        to authorize requests.
    """

    def __init__(self, credentials):
        self._credentials = credentials

    def __call__(self, ignored_val):
        """Adds authorization header to request metadata."""
        access_token = self._credentials.get_access_token().access_token
        return [('Authorization', 'Bearer ' + access_token)]


class Connection(object):
    """HTTP-RPC Connection base class for Google Cloud Bigtable.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                       :class:`NoneType`
    :param credentials: The OAuth2 Credentials to use for this connection.
    """

    USER_AGENT = 'gcloud-bigtable-python'
    SCOPE = None

    def __init__(self, credentials=None):
        credentials = self._create_scoped_credentials(
            credentials, (self.SCOPE,))
        self._credentials = credentials

    @property
    def credentials(self):
        """Getter for current credentials.

        :rtype: :class:`oauth2client.client.OAuth2Credentials` or
                :class:`NoneType`
        :returns: The credentials object associated with this connection.
        """
        return self._credentials

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


def make_stub(credentials, stub_factory, host, port):
    """Makes a stub for the an API.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials`
    :param credentials: The OAuth2 Credentials to use for access tokens
                        to authorize requests.

    :type stub_factory: callable
    :param stub_factory: A factory which will create a gRPC stub for
                         a given service.

    :type host: string
    :param host: The host for the service.

    :type port: integer
    :param port: The port for the service.

    :rtype: :class:`grpc.early_adopter.implementations._Stub`
    :returns: The stub object used to make gRPC requests to the
              Data API.
    """
    custom_metadata_transformer = MetadataTransformer(credentials)
    return stub_factory(host, port,
                        metadata_transformer=custom_metadata_transformer,
                        secure=True,
                        root_certificates=get_certs())
