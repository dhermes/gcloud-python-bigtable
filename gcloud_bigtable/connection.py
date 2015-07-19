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


from gcloud_bigtable._generated import bigtable_service_messages_pb2


# See https://gist.github.com/dhermes/bbc5b7be1932bfffae77
# for appropriate values on other systems.
SSL_CERT_FILE = '/etc/ssl/certs/ca-certificates.crt'


class AuthInfo(object):
    """Local namespace for caching auth information."""

    ROOT_CERTIFICATES = None


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
    """HTTP-RPC Connection base class for Google Cloud BigTable.

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


def _set_certs():
    """Sets the cached root certificates locally."""
    with open(SSL_CERT_FILE, mode='rb') as file_obj:
        AuthInfo.ROOT_CERTIFICATES = file_obj.read()


def set_certs(reset=False):
    """Sets the cached root certificates locally.

    If not manually told to reset or if the value is already set,
    does nothing.
    """
    if AuthInfo.ROOT_CERTIFICATES is None or reset:
        _set_certs()


def get_certs():
    """Gets the cached root certificates.

    Calls set_certs() first in case the value has not been set, but
    this will do nothing if the value is already set.
    """
    set_certs(reset=False)
    return AuthInfo.ROOT_CERTIFICATES
