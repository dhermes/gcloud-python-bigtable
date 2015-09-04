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

"""Google Cloud Bigtable HappyBase table module."""


def make_row(cell_map, include_timestamp):
    """Make a row dict for a Thrift cell mapping.

    .. note::

        This method is only provided for HappyBase compatibility, but does not
        actually work.

    :type cell_map: dict
    :param cell_map: Dictionary with ``fam:col`` strings as keys and ``TCell``
                     instances as values.

    :type include_timestamp: bool
    :param include_timestamp: Flag to indicate if cell timestamps should be
                              included with the output.

    :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
             always
    """
    raise NotImplementedError('The Cloud Bigtable API output is not the same '
                              'as the output from the Thrift server, so this '
                              'helper can not be implemented.', 'Called with',
                              cell_map, include_timestamp)


def make_ordered_row(sorted_columns, include_timestamp):
    """Make a row dict for sorted Thrift column results from scans.

    .. note::

        This method is only provided for HappyBase compatibility, but does not
        actually work.

    :type sorted_columns: list
    :param sorted_columns: List of ``TColumn`` instances from Thrift.

    :type include_timestamp: bool
    :param include_timestamp: Flag to indicate if cell timestamps should be
                              included with the output.

    :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
             always
    """
    raise NotImplementedError('The Cloud Bigtable API output is not the same '
                              'as the output from the Thrift server, so this '
                              'helper can not be implemented.', 'Called with',
                              sorted_columns, include_timestamp)


class Table(object):
    """Representation of Cloud Bigtable table.

    Used for adding data and

    :type name: str
    :param name: The name of the table.

    :type connection: :class:`.Connection`
    :param connection: The connection which has access to the table.
    """

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def __repr__(self):
        return '<table.Table name=%r>' % (self.name,)

    def families(self):
        """Retrieve the column families for this table.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def regions(self):
        """Retrieve the regions for this table.

        Cloud Bigtable does not give information about how a table is laid
        out in memory, so regions so this method does not work. It is
        provided simply for compatibility.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 always
        """
        raise NotImplementedError('The Cloud Bigtable API does not have a '
                                  'concept of splitting a table into regions.')

    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        """Retrieve a single row of data.

        Returns the latest cells in each column (or all columns if ``columns``
        is not specified). If a ``timestamp`` is set, then **latest** becomes
        **latest** up until ``timestamp``.

        :type row: str
        :param row: Row key for the row we are reading from.

        :type columns: list
        :param columns: (Optional) Iterable containing column names (as
                        strings). Each column name can be either

                          * an entire column family: ``fam`` or ``fam:``
                          * an single column: ``fam:col``

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch). If specified, only cells returned before (or
                          at) the timestamp will be returned.

        :type include_timestamp: bool
        :param include_timestamp: Flag to indicate if cell timestamps should be
                                  included with the output.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def rows(self, rows, columns=None, timestamp=None,
             include_timestamp=False):
        """Retrieve multiple rows of data.

        All optional arguments behave the same in this method as they do in
        :meth:`row`.

        :type rows: list
        :param rows: Iterable of the row keys for the rows we are reading from.

        :type columns: list
        :param columns: (Optional) Iterable containing column names (as
                        strings). Each column name can be either

                          * an entire column family: ``fam`` or ``fam:``
                          * an single column: ``fam:col``

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch). If specified, only cells returned before (or
                          at) the timestamp will be returned.

        :type include_timestamp: bool
        :param include_timestamp: Flag to indicate if cell timestamps should be
                                  included with the output.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def cells(self, row, column, versions=None, timestamp=None,
              include_timestamp=False):
        """Retrieve multiple versions of a single cell from the table.

        :type row: str
        :param row: Row key for the row we are reading from.

        :type column: str
        :param column: Column we are reading from; of the form ``fam:col``.

        :type versions: int
        :param versions: (Optional) The maximum number of cells to return. If
                         not set, returns all cells found.

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch). If specified, only cells returned before (or
                          at) the timestamp will be returned.

        :type include_timestamp: bool
        :param include_timestamp: Flag to indicate if cell timestamps should be
                                  included with the output.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def scan(self, row_start=None, row_stop=None, row_prefix=None,
             columns=None, filter=None, timestamp=None,
             include_timestamp=False, batch_size=1000, scan_batching=None,
             limit=None, sorted_columns=False):
        """Create a scanner for data in this table.

        This method returns a generator that can be used for looping over the
        matching rows.

        If ``row_prefix`` is specified, only rows with row keys matching the
        prefix will be returned. If given, ``row_start`` and ``row_stop``
        cannot be used.

        .. note::

            Both ``row_start`` and ``row_stop`` can be :data:`None` to specify
            the start and the end of the table respectively. If both are
            omitted, a full table scan is done. Note that this usually results
            in severe performance problems.

        :type row_start: str
        :param row_start: (Optional) Row key where the scanner should start
                          (includes ``row_start``). If not specified, reads
                          from the first key. If the table does not contain
                          ``row_start``, it will start from the next key after
                          it that **is** contained in the table.

        :type row_stop: str
        :param row_stop: (Optional) Row key where the scanner should stop
                         (excludes ``row_stop``). If not specified, reads
                         until the last key. The table does not have to contain
                         ``row_stop``.

        :type row_prefix: str
        :param row_prefix: (Optional) Prefix to match row keys.

        :type columns: list
        :param columns: (Optional) Iterable containing column names (as
                        strings). Each column name can be either

                          * an entire column family: ``fam`` or ``fam:``
                          * an single column: ``fam:col``

        :type filter: str
        :param filter: (Optional) An HBase filter string. See
                       http://hbase.apache.org/0.94/book/thrift.html
                       for more details.

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch). If specified, only cells returned before (or
                          at) the timestamp will be returned.

        :type include_timestamp: bool
        :param include_timestamp: Flag to indicate if cell timestamps should be
                                  included with the output.

        :type batch_size: int
        :param batch_size: Number of results to retrieve per batch. Defaults
                           to 1000. Should be kept large unless individual
                           row results are very large.

        :type scan_batching: bool
        :param scan_batching: Unused parameter. Provided for compatibility
                              with HappyBase, but irrelevant for Cloud Bigtable
                              since it does not have concepts of batching or
                              caching for scans.

        :type limit: int
        :param limit: (Optional) Maximum number of rows to return.

        :type sorted_columns: bool
        :param sorted_columns: Flag to indicate if the returned columns need
                               to be sorted.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def put(self, row, data, timestamp=None, wal=True):
        """Insert data into a row in this table.

        .. note::

            This method will send a request with a single "put" mutation.
            In many situations, :meth:`batch` is a more appropriate
            method to manipulate data since it helps combine many mutations
            into a single request.

        :type row: str
        :param row: The row key where the mutation will be "put".

        :type data: dict
        :param data: Dictionary containing the data to be inserted. The keys
                     are columns names (of the form ``fam:col``) and the values
                     are strings (bytes) to be stored in those columns.

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch) that the mutation will be applied at.

        :type wal: :data:`NoneType <types.NoneType>`
        :param wal: Unused parameter (to be passed to a created batch).
                    Provided for compatibility with HappyBase, but irrelevant
                    for Cloud Bigtable since it does not have a Write Ahead
                    Log.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def delete(self, row, columns=None, timestamp=None, wal=True):
        """Delete data from a row in this table.

        This method deletes the entire ``row`` if ``columns`` is not
        specified.

        .. note::

            This method will send a request with a single delete mutation.
            In many situations, :meth:`batch` is a more appropriate
            method to manipulate data since it helps combine many mutations
            into a single request.

        :type row: str
        :param row: The row key where the delete will occur.

        :type columns: list
        :param columns: (Optional) Iterable containing column names (as
                        strings). Each column name can be either

                          * an entire column family: ``fam`` or ``fam:``
                          * an single column: ``fam:col``

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch) that the mutation will be applied at.

        :type wal: :data:`NoneType <types.NoneType>`
        :param wal: Unused parameter (to be passed to a created batch).
                    Provided for compatibility with HappyBase, but irrelevant
                    for Cloud Bigtable since it does not have a Write Ahead
                    Log.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')

    def batch(self, timestamp=None, batch_size=None, transaction=False,
              wal=True):
        """Create a new batch operation for this table.

        This method returns a new :class:`.Batch` instance that can be used
        for mass data manipulation.

        :type timestamp: int
        :param timestamp: (Optional) Timestamp (in milliseconds since the
                          epoch) that all mutations will be applied at.

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

        :type wal: :data:`NoneType <types.NoneType>`
        :param wal: Unused parameter (to be passed to the created batch).
                    Provided for compatibility with HappyBase, but irrelevant
                    for Cloud Bigtable since it does not have a Write Ahead
                    Log.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 temporarily until the method is implemented.
        """
        raise NotImplementedError('Temporarily not implemented.')
