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


import struct

from gcloud_bigtable._helpers import _microseconds_to_timestamp
from gcloud_bigtable.column_family import GarbageCollectionRule
from gcloud_bigtable.column_family import GarbageCollectionRuleIntersection
from gcloud_bigtable.happybase.batch import Batch
from gcloud_bigtable.happybase.batch import _WAL_SENTINEL
from gcloud_bigtable.row import TimestampRange
from gcloud_bigtable.table import Table as _LowLevelTable


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


def _gc_rule_to_dict(gc_rule):
    """Converts garbage collection rule to dictionary if possible.

    This is in place to support dictionary values was was done
    in HappyBase, which has somewhat different garbage collection rule
    settings for column families.

    Only does this if the garbage collection rule is:

    * Simple :class:`.GarbageCollectionRule` with ``max_age``
    * Simple :class:`.GarbageCollectionRule` with ``max_num_versions``
    * Composite :class:`.GarbageCollectionRuleIntersection` with
      two rules each for ``max_age`` and ``max_num_versions``

    Otherwise, just returns the input without change.

    :type gc_rule: :data:`NoneType <types.NoneType>`,
                   :class:`.GarbageCollectionRule`,
                   :class:`.GarbageCollectionRuleIntersection`, or
                   :class:`.GarbageCollectionRuleUnion`
    :param gc_rule: A garbae collection rule to convert to a dictionary
                    (if possible).

    :rtype: dict,
            :class:`.GarbageCollectionRuleIntersection`, or
            :class:`.GarbageCollectionRuleUnion`
    :returns: The converted garbage collection rule.
    """
    result = gc_rule
    if gc_rule is None:
        result = {}
    elif isinstance(gc_rule, GarbageCollectionRule):
        result = {}
        # We assume that the GC rule has a single value.
        if gc_rule.max_num_versions is not None:
            result['max_versions'] = gc_rule.max_num_versions
        if gc_rule.max_age is not None:
            result['time_to_live'] = gc_rule.max_age.total_seconds()
    elif isinstance(gc_rule, GarbageCollectionRuleIntersection):
        if len(gc_rule.rules) == 2:
            rule1, rule2 = gc_rule.rules
            if (isinstance(rule1, GarbageCollectionRule) and
                    isinstance(rule2, GarbageCollectionRule)):
                rule1 = _gc_rule_to_dict(rule1)
                rule2 = _gc_rule_to_dict(rule2)
                key1, = rule1.keys()
                key2, = rule2.keys()
                if key1 != key2:
                    result = {key1: rule1[key1], key2: rule2[key2]}
    return result


def _convert_to_time_range(timestamp=None):
    """Create a timestamp range from an HBase / HappyBase timestamp.

    HBase uses timestamp as an argument to specify an inclusive end
    deadline. Since Cloud Bigtable uses exclusive end times, we increment
    by one millisecond (the lowest granularity allowed).

    :type timestamp: int
    :param timestamp: (Optional) Timestamp (in milliseconds since the
                      epoch). Intended to be used as the end of an HBase
                      time range, which is inclusive.

    :rtype: :class:`.TimestampRange`, :data:`NoneType <types.NoneType>`
    :returns: The timestamp range corresponding to the passed in
              ``timestamp``.
    """
    if timestamp is None:
        return None

    next_timestamp = _microseconds_to_timestamp(1000 * (timestamp + 1))
    return TimestampRange(end=next_timestamp)


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
        # This remains as legacy for HappyBase, but only the cluster
        # from it is needed.
        self.connection = connection
        self._low_level_table = None
        if self.connection is not None:
            self._low_level_table = _LowLevelTable(self.name,
                                                   self.connection._cluster)

    def __repr__(self):
        return '<table.Table name=%r>' % (self.name,)

    def families(self):
        """Retrieve the column families for this table.

        :rtype: dict
        :returns: Mapping from column family name to garbage collection rule
                  for a column family.
        """
        table = _LowLevelTable(self.name, self.connection._cluster)
        column_family_map = table.list_column_families()
        return {col_fam: _gc_rule_to_dict(col_fam_obj.gc_rule)
                for col_fam, col_fam_obj in column_family_map.items()}

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

    def put(self, row, data, timestamp=None, wal=_WAL_SENTINEL):
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

        :type wal: object
        :param wal: Unused parameter (to be passed to a created batch).
                    Provided for compatibility with HappyBase, but irrelevant
                    for Cloud Bigtable since it does not have a Write Ahead
                    Log.
        """
        with self.batch(timestamp=timestamp, wal=wal) as batch:
            batch.put(row, data)

    def delete(self, row, columns=None, timestamp=None, wal=_WAL_SENTINEL):
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

        :type wal: object
        :param wal: Unused parameter (to be passed to a created batch).
                    Provided for compatibility with HappyBase, but irrelevant
                    for Cloud Bigtable since it does not have a Write Ahead
                    Log.
        """
        with self.batch(timestamp=timestamp, wal=wal) as batch:
            batch.delete(row, columns)

    def batch(self, timestamp=None, batch_size=None, transaction=False,
              wal=_WAL_SENTINEL):
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

        :type wal: object
        :param wal: Unused parameter (to be passed to the created batch).
                    Provided for compatibility with HappyBase, but irrelevant
                    for Cloud Bigtable since it does not have a Write Ahead
                    Log.
        """
        return Batch(self, timestamp=timestamp, batch_size=batch_size,
                     transaction=transaction, wal=wal)

    def counter_get(self, row, column):
        """Retrieve the current value of a counter column.

        This method retrieves the current value of a counter column. If the
        counter column does not exist, this function initializes it to ``0``.

        .. note::

            Application code should **never** store a counter value directly;
            use the atomic :meth:`counter_inc` and :meth:`counter_dec` methods
            for that.

        :type row: str
        :param row: Row key for the row we are getting a counter from.

        :type column: str
        :param column: Column we are ``get``-ing from; of the form ``fam:col``.

        :rtype: int
        :returns: Counter value (after initializing / incrementing by 0).
        """
        # Don't query directly, but increment with value=0 so that the counter
        # is correctly initialised if didn't exist yet.
        return self.counter_inc(row, column, value=0)

    def counter_set(self, row, column, value=0):
        """Set a counter column to a specific value.

        This method is provided in HappyBase, but we do not provide it here
        because it defeats the purpose of using atomic increment and decrement
        of a counter.

        :type row: str
        :param row: Row key for the row we are setting a counter in.

        :type column: str
        :param column: Column we are setting a value in; of
                       the form ``fam:col``.

        :type value: int
        :param value: Value to set the counter to.

        :raises: :class:`NotImplementedError <exceptions.NotImplementedError>`
                 always
        """
        raise NotImplementedError('Table.counter_set will not be implemented. '
                                  'Instead use the increment/decrement '
                                  'methods along with counter_get.')

    def counter_inc(self, row, column, value=1):
        """Atomically increment a counter column.

        This method atomically increments a counter column in ``row``.
        If the counter column does not exist, it is automatically initialized
        to ``0`` before being incremented.

        :type row: str
        :param row: Row key for the row we are incrementing a counter in.

        :type column: str
        :param column: Column we are incrementing a value in; of the
                       form ``fam:col``.

        :type value: int
        :param value: Amount to increment the counter by. (If negative,
                      this is equivalent to decrement.)

        :rtype: int
        :returns: Counter value after incrementing.
        """
        table = _LowLevelTable(self.name, self.connection._cluster)
        row = table.row(row)
        column_family_id, column_qualifier = column.split(':')
        row.increment_cell_value(column_family_id, column_qualifier, value)
        modified_cells = row.commit_modifications()
        column_cells = modified_cells[column_family_id][column_qualifier]
        if len(column_cells) != 1:
            raise ValueError('Expected server to return one modified cell.')
        bytes_value = column_cells[0][0]
        int_value, = struct.unpack('>q', bytes_value)
        return int_value

    def counter_dec(self, row, column, value=1):
        """Atomically decrement a counter column.

        This method atomically decrements a counter column in ``row``.
        If the counter column does not exist, it is automatically initialized
        to ``0`` before being decremented.

        :type row: str
        :param row: Row key for the row we are decrementing a counter in.

        :type column: str
        :param column: Column we are decrementing a value in; of the
                       form ``fam:col``.

        :type value: int
        :param value: Amount to decrement the counter by. (If negative,
                      this is equivalent to increment.)

        :rtype: int
        :returns: Counter value after decrementing.
        """
        return self.counter_inc(row, column, -value)
