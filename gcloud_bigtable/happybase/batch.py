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

"""Google Cloud Bigtable HappyBase batch module."""


_WAL_SENTINEL = object()


class Batch(object):
    """Batch class for accumulating mutations.

    :type table: :class:`.Table`
    :param table: The table where mutations will be applied.

    :type timestamp: int
    :param timestamp: (Optional) Timestamp (in milliseconds since the epoch)
                      that all mutations will be applied at.

    :type batch_size: int
    :param batch_size: (Optional) The maximum number of mutations to allow
                       to accumulate before committing them.

    :type transaction: bool
    :param transaction: Flag indicating if the mutations should be sent
                        transactionally or not. If ``transaction=True`` and
                        an error occurs while a :class:`Batch` is active,
                        then none of the accumulated mutations will be
                        committed. If ``batch_size`` is set, the mutation
                        can't be transactional.

    :type wal: object
    :param wal: Unused parameter (Boolean for using the HBase Write Ahead Log.)
                Provided for compatibility with HappyBase, but irrelevant for
                Cloud Bigtable since it does not have a Write Ahead Log.

    :raises: :class:`TypeError <exceptions.TypeError>` if ``batch_size``
             is set and ``transaction=True``.
             :class:`ValueError <exceptions.ValueError>` if ``batch_size``
             is not positive.
    """

    def __init__(self, table, timestamp=None, batch_size=None,
                 transaction=False, wal=_WAL_SENTINEL):
        if wal is not _WAL_SENTINEL:
            raise ValueError('The wal argument cannot be used with '
                             'Cloud Bigtable.')

        if batch_size is not None:
            if transaction:
                raise TypeError('When batch_size is set, a Batch cannot be '
                                'transactional')
            if batch_size <= 0:
                raise ValueError('batch_size must be positive')

        self._table = table
        self._batch_size = batch_size
        self._timestamp = timestamp
        self._transaction = transaction
