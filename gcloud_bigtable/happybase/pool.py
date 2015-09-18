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

"""Google Cloud Bigtable HappyBase pool module."""


import six
import threading

from gcloud_bigtable.happybase.connection import Connection
from gcloud_bigtable.happybase.connection import _get_cluster


class NoConnectionsAvailable(RuntimeError):
    """Exception raised when no connections are available.

    This happens if a timeout was specified when obtaining a connection,
    and no connection became available within the specified timeout.
    """


class ConnectionPool(object):
    """Thread-safe connection pool.

    .. note::

        All keyword arguments are passed unmodified to the
        :class:`.Connection` constructor **except** for ``autoconnect``.
        This is because the ``open`` / ``closed`` status of a connection
        is managed by the pool. In addition, if ``cluster`` is not passed,
        the default / inferred cluster is determined by the pool and then
        passed to each :class:`.Connection` that is created.

    :type size: int
    :param size: The maximum number of concurrently open connections.

    :type kwargs: dict
    :param kwargs: Keyword arguments passed to :class:`.Connection`
                   constructor.

    :raises: :class:`TypeError <exceptions.TypeError>` if ``size``
             is non an integer.
             :class:`ValueError <exceptions.ValueError>` if ``size``
             is not positive.
    """
    def __init__(self, size, **kwargs):
        if not isinstance(size, six.integer_types):
            raise TypeError('Pool size arg must be an integer')

        if size <= 0:
            raise ValueError('Pool size must be positive')

        self._lock = threading.Lock()
        self._queue = six.moves.queue.LifoQueue(maxsize=size)
        self._thread_connections = threading.local()

        connection_kwargs = kwargs
        connection_kwargs['autoconnect'] = False
        if 'cluster' not in connection_kwargs:
            connection_kwargs['cluster'] = _get_cluster(
                timeout=kwargs.get('timeout'))

        for _ in six.moves.range(size):
            connection = Connection(**connection_kwargs)
            self._queue.put(connection)

    def connection(self, timeout=None):
        """Obtain a connection from the pool.

        Intended to be used as a context manager, but not implemented with
        that functionality yet.

        :type timeout: int
        :param timeout: (Optional) Time (in seconds) to wait for a connection
                        to open.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')
