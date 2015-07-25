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

"""Parent client for calling the Google Cloud Bigtable API.

This is the base from which all interactions with the API occur.

In the hierarchy of API concepts
* a client owns a cluster
* a cluster owns a table
* a table owns data
"""


ADMIN_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.admin'
"""Scope for interacting with the Cluster Admin and Table Admin APIs."""
DATA_SCOPE = 'https://www.googleapis.com/auth/cloud-bigtable.data'
"""Scope for reading and writing table data."""
READ_ONLY_SCOPE = ('https://www.googleapis.com/auth/'
                   'cloud-bigtable.data.readonly')
"""Scope for reading table data."""


class Client(object):
    """Client for interacting with Google Cloud Bigtable API.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                       :class:`NoneType`
    :param credentials: The OAuth2 Credentials to use for this cluster.

    :type read_only: boolean
    :param read_only: Boolean indicating if the data scope should be
                      for reading only (or for writing as well).

    :type admin: boolean
    :param admin: Boolean indicating if the client will be used to interact
                  with the Cluster Admin or Table Admin APIs. This requires
                  the ``ADMIN_SCOPE``.

    :raises: :class:`ValueError` if both ``read_only`` and ``admin`` are
             ``True``
    """

    def __init__(self, credentials, read_only=False,
                 admin=False):
        if read_only and admin:
            raise ValueError('A read-only client cannot also perform'
                             'administrative actions.')

        scopes = []
        if read_only:
            scopes.append(READ_ONLY_SCOPE)
        else:
            scopes.append(DATA_SCOPE)

        if admin:
            scopes.append(ADMIN_SCOPE)

        self._credentials = credentials.create_scoped(scopes)
