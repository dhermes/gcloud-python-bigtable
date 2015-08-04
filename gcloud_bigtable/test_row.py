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


import unittest2


PROJECT_ID = u'project-id'
ZONE = u'zone'
CLUSTER_ID = u'cluster-id'
TABLE_ID = u'table-id'
TABLE_NAME = ('projects/' + PROJECT_ID + '/zones/' + ZONE +
              '/clusters/' + CLUSTER_ID + '/tables/' + TABLE_ID)
ROW_KEY = b'row_key'
ROW_KEY_NON_BYTES = u'row_key'
COLUMN = b'column'
COLUMN_NON_BYTES = u'column'
COLUMN_FAMILY_ID = u'column_family_id'


class TestRow(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import Row
        return Row

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def _constructor_helper(self, row_key, row_key_expected=None, filter=None):
        table = object()
        row = self._makeOne(row_key, table, filter=filter)
        row_key_val = row_key_expected or row_key
        # Only necessary in Py2
        self.assertEqual(type(row._row_key), type(row_key_val))
        self.assertEqual(row._row_key, row_key_val)
        self.assertTrue(row._table is table)
        self.assertTrue(row._filter is filter)
        self.assertEqual(row._rule_pb_list, [])
        if filter is None:
            self.assertEqual(row._pb_mutations, [])
            self.assertTrue(row._true_pb_mutations is None)
            self.assertTrue(row._false_pb_mutations is None)
        else:
            self.assertTrue(row._pb_mutations is None)
            self.assertEqual(row._true_pb_mutations, [])
            self.assertEqual(row._false_pb_mutations, [])

    def test_constructor(self):
        self._constructor_helper(ROW_KEY)

    def test_constructor_with_filter(self):
        self._constructor_helper(ROW_KEY, filter=object())

    def test_constructor_with_unicode(self):
        self._constructor_helper(ROW_KEY_NON_BYTES, row_key_expected=ROW_KEY)

    def test_constructor_with_non_bytes(self):
        row_key = object()
        with self.assertRaises(TypeError):
            self._constructor_helper(row_key)

    def test_table_getter(self):
        table = object()
        row = self._makeOne(ROW_KEY, table)
        self.assertTrue(row.table is table)

    def test_row_key_getter(self):
        row = self._makeOne(ROW_KEY, object())
        self.assertEqual(row.row_key, ROW_KEY)

    def test_filter_getter(self):
        filter_ = object()
        row = self._makeOne(ROW_KEY, object(), filter=filter_)
        self.assertTrue(row.filter is filter_)

    def test_client_getter(self):
        client = object()
        table = _Table(None, client=client)
        row = self._makeOne(ROW_KEY, table)
        self.assertTrue(row.client is client)

    def test_timeout_seconds_getter(self):
        timeout_seconds = 889
        table = _Table(None, timeout_seconds=timeout_seconds)
        row = self._makeOne(ROW_KEY, table)
        self.assertEqual(row.timeout_seconds, timeout_seconds)

    def _get_mutations_helper(self, filter=None, state=None):
        row = self._makeOne(ROW_KEY, None, filter=filter)
        # Mock the mutations with unique objects so we can compare.
        row._pb_mutations = no_bool = object()
        row._true_pb_mutations = true_mutations = object()
        row._false_pb_mutations = false_mutations = object()

        mutations = row._get_mutations(state)
        return (no_bool, true_mutations, false_mutations), mutations

    def test__get_mutations_no_filter(self):
        (no_bool, _, _), mutations = self._get_mutations_helper()
        self.assertTrue(mutations is no_bool)

    def test__get_mutations_no_filter_bad_state(self):
        state = object()  # State should be null when no filter.
        with self.assertRaises(ValueError):
            self._get_mutations_helper(state=state)

    def test__get_mutations_with_filter_true_state(self):
        filter_ = object()
        state = True
        (_, true_filter, _), mutations = self._get_mutations_helper(
            filter=filter_, state=state)
        self.assertTrue(mutations is true_filter)

    def test__get_mutations_with_filter_false_state(self):
        filter_ = object()
        state = False
        (_, _, false_filter), mutations = self._get_mutations_helper(
            filter=filter_, state=state)
        self.assertTrue(mutations is false_filter)

    def test__get_mutations_with_filter_bad_state(self):
        filter_ = object()
        state = None
        with self.assertRaises(ValueError):
            self._get_mutations_helper(filter=filter_, state=state)

    def test_append_cell_value(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(ROW_KEY, table)
        self.assertEqual(row._rule_pb_list, [])

        value = b'bytes-val'
        row.append_cell_value(COLUMN_FAMILY_ID, COLUMN, value)
        expected_pb = data_pb2.ReadModifyWriteRule(
            family_name=COLUMN_FAMILY_ID, column_qualifier=COLUMN,
            append_value=value)
        self.assertEqual(row._rule_pb_list, [expected_pb])

    def test_increment_cell_value(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(ROW_KEY, table)
        self.assertEqual(row._rule_pb_list, [])

        int_value = 281330
        row.increment_cell_value(COLUMN_FAMILY_ID, COLUMN, int_value)
        expected_pb = data_pb2.ReadModifyWriteRule(
            family_name=COLUMN_FAMILY_ID, column_qualifier=COLUMN,
            increment_amount=int_value)
        self.assertEqual(row._rule_pb_list, [expected_pb])

    def _set_cell_helper(self, column=COLUMN, column_bytes=None,
                         value=b'foobar', timestamp=None,
                         timestamp_micros=-1):
        import six
        import struct
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(ROW_KEY, table)
        self.assertEqual(row._pb_mutations, [])
        row.set_cell(COLUMN_FAMILY_ID, column,
                     value, timestamp=timestamp)

        if isinstance(value, six.integer_types):
            value = struct.pack('>q', value)
        expected_pb = data_pb2.Mutation(
            set_cell=data_pb2.Mutation.SetCell(
                family_name=COLUMN_FAMILY_ID,
                column_qualifier=column_bytes or column,
                timestamp_micros=timestamp_micros,
                value=value,
            ),
        )
        self.assertEqual(row._pb_mutations, [expected_pb])

    def test_set_cell(self):
        self._set_cell_helper(column=COLUMN)

    def test_set_cell_with_string_column(self):
        self._set_cell_helper(column=COLUMN_NON_BYTES, column_bytes=COLUMN)

    def test_set_cell_with_integer_value(self):
        value = 1337
        self._set_cell_helper(column=COLUMN, value=value)

    def test_set_cell_with_non_bytes_value(self):
        table = object()
        row = self._makeOne(ROW_KEY, table)
        value = object()  # Not bytes
        with self.assertRaises(TypeError):
            row.set_cell(None, None, value)

    def test_set_cell_with_non_null_timestamp(self):
        import datetime
        from gcloud_bigtable. _helpers import EPOCH

        microseconds = 898294371
        millis_granularity = microseconds - (microseconds % 1000)
        timestamp = EPOCH + datetime.timedelta(microseconds=microseconds)
        self._set_cell_helper(timestamp=timestamp,
                              timestamp_micros=millis_granularity)

    def test_delete(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        row = self._makeOne(ROW_KEY, object())
        self.assertEqual(row._pb_mutations, [])
        row.delete()

        expected_pb = data_pb2.Mutation(
            delete_from_row=data_pb2.Mutation.DeleteFromRow(),
        )
        self.assertEqual(row._pb_mutations, [expected_pb])

    def test_delete_cell(self):
        klass = self._getTargetClass()

        class MockRow(klass):

            def __init__(self, *args, **kwargs):
                super(MockRow, self).__init__(*args, **kwargs)
                self._args = []
                self._kwargs = []

            # Replace the called method with one that logs arguments.
            def delete_cells(self, *args, **kwargs):
                self._args.append(args)
                self._kwargs.append(kwargs)

        table = object()
        mock_row = MockRow(ROW_KEY, table)
        # Make sure no values are set before calling the method.
        self.assertEqual(mock_row._pb_mutations, [])
        self.assertEqual(mock_row._args, [])
        self.assertEqual(mock_row._kwargs, [])

        # Actually make the request against the mock class.
        time_range = object()
        mock_row.delete_cell(COLUMN_FAMILY_ID, COLUMN, time_range=time_range)
        self.assertEqual(mock_row._pb_mutations, [])
        self.assertEqual(mock_row._args, [(COLUMN_FAMILY_ID, [COLUMN])])
        self.assertEqual(mock_row._kwargs, [{
            'state': None,
            'time_range': time_range,
        }])

    def test_delete_cells_non_iterable(self):
        table = object()
        row = self._makeOne(ROW_KEY, table)
        columns = object()  # Not iterable
        with self.assertRaises(TypeError):
            row.delete_cells(COLUMN_FAMILY_ID, columns)

    def test_delete_cells_all_columns(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(ROW_KEY, table)
        klass = self._getTargetClass()
        self.assertEqual(row._pb_mutations, [])
        row.delete_cells(COLUMN_FAMILY_ID, klass.ALL_COLUMNS)

        expected_pb = data_pb2.Mutation(
            delete_from_family=data_pb2.Mutation.DeleteFromFamily(
                family_name=COLUMN_FAMILY_ID,
            ),
        )
        self.assertEqual(row._pb_mutations, [expected_pb])

    def test_delete_cells_no_columns(self):
        table = object()
        row = self._makeOne(ROW_KEY, table)
        columns = []
        self.assertEqual(row._pb_mutations, [])
        row.delete_cells(COLUMN_FAMILY_ID, columns)
        self.assertEqual(row._pb_mutations, [])

    def _delete_cells_helper(self, time_range=None):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(ROW_KEY, table)
        columns = [COLUMN]
        self.assertEqual(row._pb_mutations, [])
        row.delete_cells(COLUMN_FAMILY_ID, columns, time_range=time_range)

        expected_pb = data_pb2.Mutation(
            delete_from_column=data_pb2.Mutation.DeleteFromColumn(
                family_name=COLUMN_FAMILY_ID,
                column_qualifier=COLUMN,
            ),
        )
        if time_range is not None:
            expected_pb.delete_from_column.time_range.CopyFrom(
                time_range.to_pb())
        self.assertEqual(row._pb_mutations, [expected_pb])

    def test_delete_cells_no_time_range(self):
        self._delete_cells_helper()

    def test_delete_cells_with_time_range(self):
        import datetime
        from gcloud_bigtable. _helpers import EPOCH
        from gcloud_bigtable.row import TimestampRange

        microseconds = 30871000  # Makes sure already milliseconds granularity
        start = EPOCH + datetime.timedelta(microseconds=microseconds)
        time_range = TimestampRange(start=start)
        self._delete_cells_helper(time_range=time_range)

    def test_delete_cells_with_bad_column(self):
        # This makes sure a failure on one of the columns doesn't leave
        # the row's mutations in a bad state.
        table = object()
        row = self._makeOne(ROW_KEY, table)
        columns = [COLUMN, object()]
        self.assertEqual(row._pb_mutations, [])
        with self.assertRaises(TypeError):
            row.delete_cells(COLUMN_FAMILY_ID, columns)
        self.assertEqual(row._pb_mutations, [])

    def test_delete_cells_with_string_columns(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        table = object()
        row = self._makeOne(ROW_KEY, table)
        column1 = u'column1'
        column1_bytes = b'column1'
        column2 = u'column2'
        column2_bytes = b'column2'
        columns = [column1, column2]
        self.assertEqual(row._pb_mutations, [])
        row.delete_cells(COLUMN_FAMILY_ID, columns)

        expected_pb1 = data_pb2.Mutation(
            delete_from_column=data_pb2.Mutation.DeleteFromColumn(
                family_name=COLUMN_FAMILY_ID,
                column_qualifier=column1_bytes,
            ),
        )
        expected_pb2 = data_pb2.Mutation(
            delete_from_column=data_pb2.Mutation.DeleteFromColumn(
                family_name=COLUMN_FAMILY_ID,
                column_qualifier=column2_bytes,
            ),
        )
        self.assertEqual(row._pb_mutations, [expected_pb1, expected_pb2])

    def test_commit(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._generated import empty_pb2
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client()
        table = _Table(TABLE_NAME, client=client)
        row = self._makeOne(ROW_KEY, table)

        # Create request_pb
        value = b'bytes-value'
        mutation = data_pb2.Mutation(
            set_cell=data_pb2.Mutation.SetCell(
                family_name=COLUMN_FAMILY_ID,
                column_qualifier=COLUMN,
                timestamp_micros=-1,  # Default value.
                value=value,
            ),
        )
        request_pb = messages_pb2.MutateRowRequest(
            table_name=TABLE_NAME,
            row_key=ROW_KEY,
            mutations=[mutation],
        )

        # Create response_pb
        response_pb = empty_pb2.Empty()

        # Patch the stub used by the API method.
        client.data_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = None  # commit() has no return value when no filter.

        # Perform the method and check the result.
        timeout_seconds = 711
        row.set_cell(COLUMN_FAMILY_ID, COLUMN, value)
        result = row.commit(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'MutateRow',
            (request_pb, timeout_seconds),
            {},
        )])
        self.assertEqual(row._pb_mutations, [])
        self.assertEqual(row._true_pb_mutations, None)
        self.assertEqual(row._false_pb_mutations, None)

    def test_commit_too_many_mutations(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import row as MUT

        table = object()
        row = self._makeOne(ROW_KEY, table)
        row._pb_mutations = [1, 2, 3]
        num_mutations = len(row._pb_mutations)
        with _Monkey(MUT, _MAX_MUTATIONS=num_mutations - 1):
            with self.assertRaises(ValueError):
                row.commit()

    def test_commit_no_mutations(self):
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client()
        table = _Table(None, client=client)
        row = self._makeOne(ROW_KEY, table)
        self.assertEqual(row._pb_mutations, [])

        # Patch the stub used by the API method.
        client.data_stub = stub = StubMock()

        # Perform the method and check the result.
        result = row.commit()
        self.assertEqual(result, None)
        # Make sure no request was sent.
        self.assertEqual(stub.method_calls, [])

    def test_commit_with_filter(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable.row import RowFilter

        client = _Client()
        table = _Table(TABLE_NAME, client=client)
        row_filter = RowFilter(row_sample_filter=0.33)
        row = self._makeOne(ROW_KEY, table, filter=row_filter)

        # Create request_pb
        value = b'bytes-value'
        mutation = data_pb2.Mutation(
            set_cell=data_pb2.Mutation.SetCell(
                family_name=COLUMN_FAMILY_ID,
                column_qualifier=COLUMN,
                timestamp_micros=-1,  # Default value.
                value=value,
            ),
        )
        request_pb = messages_pb2.CheckAndMutateRowRequest(
            table_name=TABLE_NAME,
            row_key=ROW_KEY,
            predicate_filter=row_filter.to_pb(),
            true_mutations=[mutation],
            false_mutations=[],
        )

        # Create response_pb
        predicate_matched = True
        response_pb = messages_pb2.CheckAndMutateRowResponse(
            predicate_matched=predicate_matched)

        # Patch the stub used by the API method.
        client.data_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = predicate_matched

        # Perform the method and check the result.
        timeout_seconds = 262
        row.set_cell(COLUMN_FAMILY_ID, COLUMN, value, state=True)
        result = row.commit(timeout_seconds=timeout_seconds)
        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'CheckAndMutateRow',
            (request_pb, timeout_seconds),
            {},
        )])
        self.assertEqual(row._pb_mutations, None)
        self.assertEqual(row._true_pb_mutations, [])
        self.assertEqual(row._false_pb_mutations, [])

    def test_commit_with_filter_too_many_mutations(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import row as MUT

        table = object()
        filter_ = object()
        row = self._makeOne(ROW_KEY, table, filter=filter_)
        row._true_pb_mutations = [1, 2, 3]
        num_mutations = len(row._true_pb_mutations)
        with _Monkey(MUT, _MAX_MUTATIONS=num_mutations - 1):
            with self.assertRaises(ValueError):
                row.commit()

    def test_commit_with_filter_no_mutations(self):
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client()
        table = _Table(None, client=client)
        filter_ = object()
        row = self._makeOne(ROW_KEY, table, filter=filter_)
        self.assertEqual(row._true_pb_mutations, [])
        self.assertEqual(row._false_pb_mutations, [])

        # Patch the stub used by the API method.
        client.data_stub = stub = StubMock()

        # Perform the method and check the result.
        result = row.commit()
        self.assertEqual(result, None)
        # Make sure no request was sent.
        self.assertEqual(stub.method_calls, [])

    def test_commit_modifications(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._generated import (
            bigtable_service_messages_pb2 as messages_pb2)
        from gcloud_bigtable._grpc_mocks import StubMock
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import row as MUT

        client = _Client()
        table = _Table(TABLE_NAME, client=client)
        row = self._makeOne(ROW_KEY, table)

        # Create request_pb
        value = b'bytes-value'
        # We will call row.append_cell_value(COLUMN_FAMILY_ID, COLUMN, value).
        request_pb = messages_pb2.ReadModifyWriteRowRequest(
            table_name=TABLE_NAME,
            row_key=ROW_KEY,
            rules=[
                data_pb2.ReadModifyWriteRule(
                    family_name=COLUMN_FAMILY_ID,
                    column_qualifier=COLUMN,
                    append_value=value,
                ),
            ],
        )

        # Create response_pb
        response_pb = object()

        # Patch the stub used by the API method.
        client.data_stub = stub = StubMock(response_pb)

        # Create expected_result.
        expected_result = object()
        mock_parse_rmw_row_response = _MockCalled(expected_result)

        # Perform the method and check the result.
        timeout_seconds = 87

        with _Monkey(MUT, _parse_rmw_row_response=mock_parse_rmw_row_response):
            row.append_cell_value(COLUMN_FAMILY_ID, COLUMN, value)
            result = row.commit_modifications(timeout_seconds=timeout_seconds)

        self.assertEqual(result, expected_result)
        self.assertEqual(stub.method_calls, [(
            'ReadModifyWriteRow',
            (request_pb, timeout_seconds),
            {},
        )])
        self.assertEqual(row._pb_mutations, [])
        self.assertEqual(row._true_pb_mutations, None)
        self.assertEqual(row._false_pb_mutations, None)

        mock_parse_rmw_row_response.check_called(self, [(response_pb,)])
        self.assertEqual(row._rule_pb_list, [])

    def test_commit_modifications_no_rules(self):
        from gcloud_bigtable._grpc_mocks import StubMock

        client = _Client()
        table = _Table(None, client=client)
        row = self._makeOne(ROW_KEY, table)
        self.assertEqual(row._rule_pb_list, [])

        # Patch the stub used by the API method.
        client.data_stub = stub = StubMock()

        # Perform the method and check the result.
        result = row.commit_modifications()
        self.assertEqual(result, {})
        # Make sure no request was sent.
        self.assertEqual(stub.method_calls, [])


class Test__parse_rmw_row_response(unittest2.TestCase):

    def _callFUT(self, row_response):
        from gcloud_bigtable.row import _parse_rmw_row_response
        return _parse_rmw_row_response(row_response)

    def test_it(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._helpers import _microseconds_to_timestamp

        COL_FAM1 = u'col-fam-id'
        COL_FAM2 = u'col-fam-id2'
        COL_NAME1 = b'col-name1'
        COL_NAME2 = b'col-name2'
        COL_NAME3 = b'col-name3-but-other-fam'
        CELL_VAL1 = b'cell-val'
        CELL_VAL2 = b'cell-val-newer'
        CELL_VAL3 = b'altcol-cell-val'
        CELL_VAL4 = b'foo'

        microseconds = 1000871
        timestamp = _microseconds_to_timestamp(microseconds)
        expected_output = {
            COL_FAM1: {
                COL_NAME1: [
                    (CELL_VAL1, timestamp),
                    (CELL_VAL2, timestamp),
                ],
                COL_NAME2: [
                    (CELL_VAL3, timestamp),
                ],
            },
            COL_FAM2: {
                COL_NAME3: [
                    (CELL_VAL4, timestamp),
                ],
            },
        }
        sample_input = data_pb2.Row(
            families=[
                data_pb2.Family(
                    name=COL_FAM1,
                    columns=[
                        data_pb2.Column(
                            qualifier=COL_NAME1,
                            cells=[
                                data_pb2.Cell(
                                    value=CELL_VAL1,
                                    timestamp_micros=microseconds,
                                ),
                                data_pb2.Cell(
                                    value=CELL_VAL2,
                                    timestamp_micros=microseconds,
                                ),
                            ],
                        ),
                        data_pb2.Column(
                            qualifier=COL_NAME2,
                            cells=[
                                data_pb2.Cell(
                                    value=CELL_VAL3,
                                    timestamp_micros=microseconds,
                                ),
                            ],
                        ),
                    ],
                ),
                data_pb2.Family(
                    name=COL_FAM2,
                    columns=[
                        data_pb2.Column(
                            qualifier=COL_NAME3,
                            cells=[
                                data_pb2.Cell(
                                    value=CELL_VAL4,
                                    timestamp_micros=microseconds,
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )
        self.assertEqual(expected_output, self._callFUT(sample_input))


class TestRowFilter(unittest2.TestCase):

    _PROPERTIES = (
        'row_key_regex_filter',
        'family_name_regex_filter',
        'column_qualifier_regex_filter',
        'value_regex_filter',
        'column_range_filter',
        'timestamp_range_filter',
        'value_range_filter',
        'cells_per_row_offset_filter',
        'cells_per_row_limit_filter',
        'cells_per_column_limit_filter',
        'row_sample_filter',
        'strip_value_transformer',
    )

    def _getTargetClass(self):
        from gcloud_bigtable.row import RowFilter
        return RowFilter

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        # Fails unless exactly one property is passed.
        with self.assertRaises(TypeError):
            self._makeOne()

    def test_constructor_too_many_values(self):
        # Fails unless exactly one property is passed.
        with self.assertRaises(TypeError):
            self._makeOne(row_key_regex_filter=b'value',
                          cells_per_column_limit_filter=10)

    def _row_filter_values_check(self, row_filter, value_name, value):
        for prop_name in self._PROPERTIES:
            prop_value = getattr(row_filter, prop_name)
            if prop_name == value_name:
                self.assertEqual(prop_value, value)
            else:
                self.assertTrue(prop_value is None)

    def test_constructor_row_key_regex_filter(self):
        value = b'row-key-regex'
        row_filter = self._makeOne(row_key_regex_filter=value)
        self._row_filter_values_check(
            row_filter, 'row_key_regex_filter', value)

    def test_constructor_family_name_regex_filter(self):
        value = u'family-regex'
        row_filter = self._makeOne(family_name_regex_filter=value)
        self._row_filter_values_check(
            row_filter, 'family_name_regex_filter', value)

    def test_constructor_column_qualifier_regex_filter(self):
        value = b'column-regex'
        row_filter = self._makeOne(column_qualifier_regex_filter=value)
        self._row_filter_values_check(
            row_filter, 'column_qualifier_regex_filter', value)

    def test_constructor_value_regex_filter(self):
        value = b'value-regex'
        row_filter = self._makeOne(value_regex_filter=value)
        self._row_filter_values_check(row_filter, 'value_regex_filter', value)

    def test_constructor_column_range_filter(self):
        from gcloud_bigtable.row import ColumnRange
        value = ColumnRange(COLUMN_FAMILY_ID)
        row_filter = self._makeOne(column_range_filter=value)
        self._row_filter_values_check(row_filter, 'column_range_filter',
                                      value)

    def test_constructor_timestamp_range_filter(self):
        from gcloud_bigtable.row import TimestampRange
        value = TimestampRange()
        row_filter = self._makeOne(timestamp_range_filter=value)
        self._row_filter_values_check(row_filter, 'timestamp_range_filter',
                                      value)

    def test_constructor_value_range_filter(self):
        from gcloud_bigtable.row import CellValueRange
        value = CellValueRange()
        row_filter = self._makeOne(value_range_filter=value)
        self._row_filter_values_check(row_filter, 'value_range_filter',
                                      value)

    def test_constructor_cells_per_row_offset_filter(self):
        value = 76
        row_filter = self._makeOne(cells_per_row_offset_filter=value)
        self._row_filter_values_check(
            row_filter, 'cells_per_row_offset_filter', value)

    def test_constructor_cells_per_row_limit_filter(self):
        value = 189
        row_filter = self._makeOne(cells_per_row_limit_filter=value)
        self._row_filter_values_check(
            row_filter, 'cells_per_row_limit_filter', value)

    def test_constructor_cells_per_column_limit_filter(self):
        value = 10
        row_filter = self._makeOne(cells_per_column_limit_filter=value)
        self._row_filter_values_check(
            row_filter, 'cells_per_column_limit_filter', value)

    def test_constructor_row_sample_filter(self):
        value = 0.25
        row_filter = self._makeOne(row_sample_filter=value)
        self._row_filter_values_check(row_filter, 'row_sample_filter', value)

    def test_constructor_strip_value_transformer(self):
        value = True
        row_filter = self._makeOne(strip_value_transformer=value)
        self._row_filter_values_check(row_filter, 'strip_value_transformer',
                                      value)

    def test___eq__(self):
        # Fool the constructor by passing exactly 1 value.
        row_filter1 = self._makeOne(strip_value_transformer=True)
        row_filter2 = self._makeOne(strip_value_transformer=True)
        # Set every value so we can compare them all.
        for prop_name in self._PROPERTIES:
            fake_val = object()
            setattr(row_filter1, prop_name, fake_val)
            setattr(row_filter2, prop_name, fake_val)
        self.assertEqual(row_filter1, row_filter2)

    def test___eq__type_differ(self):
        # Fool the constructor by passing exactly 1 value.
        row_filter1 = self._makeOne(strip_value_transformer=True)
        row_filter2 = object()
        self.assertNotEqual(row_filter1, row_filter2)

    def test___ne__same_value(self):
        # Fool the constructor by passing exactly 1 value.
        row_filter1 = self._makeOne(strip_value_transformer=True)
        row_filter2 = self._makeOne(strip_value_transformer=True)
        comparison_val = (row_filter1 != row_filter2)
        self.assertFalse(comparison_val)

    def test_to_pb_empty(self):
        # Fool the constructor by passing exactly 1 value.
        row_filter = self._makeOne(strip_value_transformer=True)
        # Make it artificially empty after the fact.
        row_filter.strip_value_transformer = None

        with self.assertRaises(TypeError):
            row_filter.to_pb()

    def _to_pb_test_helper(self, **kwargs):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        row_filter = self._makeOne(**kwargs)

        pb_val = row_filter.to_pb()
        expected_pb = data_pb2.RowFilter(**kwargs)
        self.assertEqual(pb_val, expected_pb)

    def test_to_pb_with_row_key_regex_filter(self):
        value = b'row-key-regex'
        self._to_pb_test_helper(row_key_regex_filter=value)

    def test_to_pb_with_family_name_regex_filter(self):
        value = u'family-regex'
        self._to_pb_test_helper(family_name_regex_filter=value)

    def test_to_pb_with_column_qualifier_regex_filter(self):
        value = b'column-regex'
        self._to_pb_test_helper(column_qualifier_regex_filter=value)

    def test_to_pb_with_value_regex_filter(self):
        value = b'value-regex'
        self._to_pb_test_helper(value_regex_filter=value)

    def test_to_pb_with_column_range_filter(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import ColumnRange

        value = ColumnRange(COLUMN_FAMILY_ID)
        row_filter = self._makeOne(column_range_filter=value)

        pb_val = row_filter.to_pb()
        expected_pb = data_pb2.RowFilter(column_range_filter=value.to_pb())
        self.assertEqual(pb_val, expected_pb)

    def test_to_pb_with_timestamp_range_filter(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import TimestampRange

        value = TimestampRange()
        row_filter = self._makeOne(timestamp_range_filter=value)

        pb_val = row_filter.to_pb()
        expected_pb = data_pb2.RowFilter(timestamp_range_filter=value.to_pb())
        self.assertEqual(pb_val, expected_pb)

    def test_to_pb_with_value_range_filter(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import CellValueRange

        value = CellValueRange()
        row_filter = self._makeOne(value_range_filter=value)

        pb_val = row_filter.to_pb()
        expected_pb = data_pb2.RowFilter(value_range_filter=value.to_pb())
        self.assertEqual(pb_val, expected_pb)

    def test_to_pb_with_cells_per_row_offset_filter(self):
        value = 76
        self._to_pb_test_helper(cells_per_row_offset_filter=value)

    def test_to_pb_with_cells_per_row_limit_filter(self):
        value = 189
        self._to_pb_test_helper(cells_per_row_limit_filter=value)

    def test_to_pb_with_cells_per_column_limit_filter(self):
        value = 10
        self._to_pb_test_helper(cells_per_column_limit_filter=value)

    def test_to_pb_with_row_sample_filter(self):
        value = 0.25
        self._to_pb_test_helper(row_sample_filter=value)

    def test_to_pb_with_strip_value_transformer(self):
        value = True
        self._to_pb_test_helper(strip_value_transformer=value)


class TestTimestampRange(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import TimestampRange
        return TimestampRange

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        start = object()
        end = object()
        time_range = self._makeOne(start=start, end=end)
        self.assertTrue(time_range.start is start)
        self.assertTrue(time_range.end is end)

    def test___eq__(self):
        start = object()
        end = object()
        time_range1 = self._makeOne(start=start, end=end)
        time_range2 = self._makeOne(start=start, end=end)
        self.assertEqual(time_range1, time_range2)

    def test___eq__type_differ(self):
        start = object()
        end = object()
        time_range1 = self._makeOne(start=start, end=end)
        time_range2 = object()
        self.assertNotEqual(time_range1, time_range2)

    def test___ne__same_value(self):
        start = object()
        end = object()
        time_range1 = self._makeOne(start=start, end=end)
        time_range2 = self._makeOne(start=start, end=end)
        comparison_val = (time_range1 != time_range2)
        self.assertFalse(comparison_val)

    def _to_pb_helper(self, start_micros=None, end_micros=None):
        import datetime
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable. _helpers import EPOCH

        pb_kwargs = {}

        start = None
        if start_micros is not None:
            start = EPOCH + datetime.timedelta(microseconds=start_micros)
            pb_kwargs['start_timestamp_micros'] = start_micros
        end = None
        if end_micros is not None:
            end = EPOCH + datetime.timedelta(microseconds=end_micros)
            pb_kwargs['end_timestamp_micros'] = end_micros
        time_range = self._makeOne(start=start, end=end)

        expected_pb = data_pb2.TimestampRange(**pb_kwargs)
        self.assertEqual(time_range.to_pb(), expected_pb)

    def test_to_pb(self):
        # Makes sure already milliseconds granularity
        start_micros = 30871000
        end_micros = 12939371000
        self._to_pb_helper(start_micros=start_micros,
                           end_micros=end_micros)

    def test_to_pb_just_start(self):
        # Makes sure already milliseconds granularity
        start_micros = 30871000
        self._to_pb_helper(start_micros=start_micros)

    def test_to_pb_just_end(self):
        # Makes sure already milliseconds granularity
        end_micros = 12939371000
        self._to_pb_helper(end_micros=end_micros)


class TestColumnRange(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import ColumnRange
        return ColumnRange

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        column_family_id = object()
        col_range = self._makeOne(column_family_id)
        self.assertTrue(col_range.column_family_id is column_family_id)
        self.assertEqual(col_range.start_column, None)
        self.assertEqual(col_range.end_column, None)
        self.assertTrue(col_range.inclusive_start)
        self.assertTrue(col_range.inclusive_end)

    def test_constructor_explicit(self):
        column_family_id = object()
        start_column = object()
        end_column = object()
        inclusive_start = object()
        inclusive_end = object()
        col_range = self._makeOne(column_family_id, start_column=start_column,
                                  end_column=end_column,
                                  inclusive_start=inclusive_start,
                                  inclusive_end=inclusive_end)
        self.assertTrue(col_range.column_family_id is column_family_id)
        self.assertTrue(col_range.start_column is start_column)
        self.assertTrue(col_range.end_column is end_column)
        self.assertTrue(col_range.inclusive_start is inclusive_start)
        self.assertTrue(col_range.inclusive_end is inclusive_end)

    def test___eq__(self):
        column_family_id = object()
        start_column = object()
        end_column = object()
        inclusive_start = object()
        inclusive_end = object()
        col_range1 = self._makeOne(column_family_id, start_column=start_column,
                                   end_column=end_column,
                                   inclusive_start=inclusive_start,
                                   inclusive_end=inclusive_end)
        col_range2 = self._makeOne(column_family_id, start_column=start_column,
                                   end_column=end_column,
                                   inclusive_start=inclusive_start,
                                   inclusive_end=inclusive_end)
        self.assertEqual(col_range1, col_range2)

    def test___eq__type_differ(self):
        column_family_id = object()
        col_range1 = self._makeOne(column_family_id)
        col_range2 = object()
        self.assertNotEqual(col_range1, col_range2)

    def test___ne__same_value(self):
        column_family_id = object()
        col_range1 = self._makeOne(column_family_id)
        col_range2 = self._makeOne(column_family_id)
        comparison_val = (col_range1 != col_range2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        col_range = self._makeOne(COLUMN_FAMILY_ID)
        expected_pb = data_pb2.ColumnRange(family_name=COLUMN_FAMILY_ID)
        self.assertEqual(col_range.to_pb(), expected_pb)

    def test_to_pb_inclusive_start(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        col_range = self._makeOne(COLUMN_FAMILY_ID, start_column=COLUMN)
        expected_pb = data_pb2.ColumnRange(
            family_name=COLUMN_FAMILY_ID,
            start_qualifier_inclusive=COLUMN,
        )
        self.assertEqual(col_range.to_pb(), expected_pb)

    def test_to_pb_exclusive_start(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        col_range = self._makeOne(COLUMN_FAMILY_ID, start_column=COLUMN,
                                  inclusive_start=False)
        expected_pb = data_pb2.ColumnRange(
            family_name=COLUMN_FAMILY_ID,
            start_qualifier_exclusive=COLUMN,
        )
        self.assertEqual(col_range.to_pb(), expected_pb)

    def test_to_pb_inclusive_end(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        col_range = self._makeOne(COLUMN_FAMILY_ID, end_column=COLUMN)
        expected_pb = data_pb2.ColumnRange(
            family_name=COLUMN_FAMILY_ID,
            end_qualifier_inclusive=COLUMN,
        )
        self.assertEqual(col_range.to_pb(), expected_pb)

    def test_to_pb_exclusive_end(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        col_range = self._makeOne(COLUMN_FAMILY_ID, end_column=COLUMN,
                                  inclusive_end=False)
        expected_pb = data_pb2.ColumnRange(
            family_name=COLUMN_FAMILY_ID,
            end_qualifier_exclusive=COLUMN,
        )
        self.assertEqual(col_range.to_pb(), expected_pb)


class TestCellValueRange(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import CellValueRange
        return CellValueRange

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor_defaults(self):
        val_range = self._makeOne()
        self.assertEqual(val_range.start_value, None)
        self.assertEqual(val_range.end_value, None)
        self.assertTrue(val_range.inclusive_start)
        self.assertTrue(val_range.inclusive_end)

    def test_constructor_explicit(self):
        start_value = object()
        end_value = object()
        inclusive_start = object()
        inclusive_end = object()
        val_range = self._makeOne(start_value=start_value, end_value=end_value,
                                  inclusive_start=inclusive_start,
                                  inclusive_end=inclusive_end)
        self.assertTrue(val_range.start_value is start_value)
        self.assertTrue(val_range.end_value is end_value)
        self.assertTrue(val_range.inclusive_start is inclusive_start)
        self.assertTrue(val_range.inclusive_end is inclusive_end)

    def test___eq__(self):
        start_value = object()
        end_value = object()
        inclusive_start = object()
        inclusive_end = object()
        val_range1 = self._makeOne(start_value=start_value,
                                   end_value=end_value,
                                   inclusive_start=inclusive_start,
                                   inclusive_end=inclusive_end)
        val_range2 = self._makeOne(start_value=start_value,
                                   end_value=end_value,
                                   inclusive_start=inclusive_start,
                                   inclusive_end=inclusive_end)
        self.assertEqual(val_range1, val_range2)

    def test___eq__type_differ(self):
        val_range1 = self._makeOne()
        val_range2 = object()
        self.assertNotEqual(val_range1, val_range2)

    def test___ne__same_value(self):
        val_range1 = self._makeOne()
        val_range2 = self._makeOne()
        comparison_val = (val_range1 != val_range2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        val_range = self._makeOne()
        expected_pb = data_pb2.ValueRange()
        self.assertEqual(val_range.to_pb(), expected_pb)

    def test_to_pb_inclusive_start(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        value = b'some-value'
        val_range = self._makeOne(start_value=value)
        expected_pb = data_pb2.ValueRange(start_value_inclusive=value)
        self.assertEqual(val_range.to_pb(), expected_pb)

    def test_to_pb_exclusive_start(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        value = b'some-value'
        val_range = self._makeOne(start_value=value, inclusive_start=False)
        expected_pb = data_pb2.ValueRange(start_value_exclusive=value)
        self.assertEqual(val_range.to_pb(), expected_pb)

    def test_to_pb_inclusive_end(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        value = b'some-value'
        val_range = self._makeOne(end_value=value)
        expected_pb = data_pb2.ValueRange(end_value_inclusive=value)
        self.assertEqual(val_range.to_pb(), expected_pb)

    def test_to_pb_exclusive_end(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        value = b'some-value'
        val_range = self._makeOne(end_value=value, inclusive_end=False)
        expected_pb = data_pb2.ValueRange(end_value_exclusive=value)
        self.assertEqual(val_range.to_pb(), expected_pb)


class TestRowFilterChain(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import RowFilterChain
        return RowFilterChain

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        filters = object()
        filter_chain = self._makeOne(filters=filters)
        self.assertTrue(filter_chain.filters is filters)

    def test___eq__(self):
        filters = object()
        row_filter1 = self._makeOne(filters=filters)
        row_filter2 = self._makeOne(filters=filters)
        self.assertEqual(row_filter1, row_filter2)

    def test___eq__type_differ(self):
        filters = object()
        row_filter1 = self._makeOne(filters=filters)
        row_filter2 = object()
        self.assertNotEqual(row_filter1, row_filter2)

    def test___ne__same_value(self):
        filters = object()
        row_filter1 = self._makeOne(filters=filters)
        row_filter2 = self._makeOne(filters=filters)
        comparison_val = (row_filter1 != row_filter2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter1_pb = row_filter1.to_pb()

        row_filter2 = RowFilter(row_sample_filter=0.25)
        row_filter2_pb = row_filter2.to_pb()

        row_filter3 = self._makeOne(filters=[row_filter1, row_filter2])
        filter_pb = row_filter3.to_pb()

        expected_pb = data_pb2.RowFilter(
            chain=data_pb2.RowFilter.Chain(
                filters=[row_filter1_pb, row_filter2_pb],
            ),
        )
        self.assertEqual(filter_pb, expected_pb)

    def test_to_pb_nested(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter2 = RowFilter(row_sample_filter=0.25)

        row_filter3 = self._makeOne(filters=[row_filter1, row_filter2])
        row_filter3_pb = row_filter3.to_pb()

        row_filter4 = RowFilter(cells_per_row_limit_filter=11)
        row_filter4_pb = row_filter4.to_pb()

        row_filter5 = self._makeOne(filters=[row_filter3, row_filter4])
        filter_pb = row_filter5.to_pb()

        expected_pb = data_pb2.RowFilter(
            chain=data_pb2.RowFilter.Chain(
                filters=[row_filter3_pb, row_filter4_pb],
            ),
        )
        self.assertEqual(filter_pb, expected_pb)


class TestRowFilterUnion(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import RowFilterUnion
        return RowFilterUnion

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        filters = object()
        filter_union = self._makeOne(filters=filters)
        self.assertTrue(filter_union.filters is filters)

    def test___eq__(self):
        filters = object()
        row_filter1 = self._makeOne(filters=filters)
        row_filter2 = self._makeOne(filters=filters)
        self.assertEqual(row_filter1, row_filter2)

    def test___eq__type_differ(self):
        filters = object()
        row_filter1 = self._makeOne(filters=filters)
        row_filter2 = object()
        self.assertNotEqual(row_filter1, row_filter2)

    def test___ne__same_value(self):
        filters = object()
        row_filter1 = self._makeOne(filters=filters)
        row_filter2 = self._makeOne(filters=filters)
        comparison_val = (row_filter1 != row_filter2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter1_pb = row_filter1.to_pb()

        row_filter2 = RowFilter(row_sample_filter=0.25)
        row_filter2_pb = row_filter2.to_pb()

        row_filter3 = self._makeOne(filters=[row_filter1, row_filter2])
        filter_pb = row_filter3.to_pb()

        expected_pb = data_pb2.RowFilter(
            interleave=data_pb2.RowFilter.Interleave(
                filters=[row_filter1_pb, row_filter2_pb],
            ),
        )
        self.assertEqual(filter_pb, expected_pb)

    def test_to_pb_nested(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter2 = RowFilter(row_sample_filter=0.25)

        row_filter3 = self._makeOne(filters=[row_filter1, row_filter2])
        row_filter3_pb = row_filter3.to_pb()

        row_filter4 = RowFilter(cells_per_row_limit_filter=11)
        row_filter4_pb = row_filter4.to_pb()

        row_filter5 = self._makeOne(filters=[row_filter3, row_filter4])
        filter_pb = row_filter5.to_pb()

        expected_pb = data_pb2.RowFilter(
            interleave=data_pb2.RowFilter.Interleave(
                filters=[row_filter3_pb, row_filter4_pb],
            ),
        )
        self.assertEqual(filter_pb, expected_pb)


class TestConditionalRowFilter(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.row import ConditionalRowFilter
        return ConditionalRowFilter

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        base_filter = object()
        true_filter = object()
        false_filter = object()
        cond_filter = self._makeOne(base_filter,
                                    true_filter=true_filter,
                                    false_filter=false_filter)
        self.assertTrue(cond_filter.base_filter is base_filter)
        self.assertTrue(cond_filter.true_filter is true_filter)
        self.assertTrue(cond_filter.false_filter is false_filter)

    def test___eq__(self):
        base_filter = object()
        true_filter = object()
        false_filter = object()
        cond_filter1 = self._makeOne(base_filter,
                                     true_filter=true_filter,
                                     false_filter=false_filter)
        cond_filter2 = self._makeOne(base_filter,
                                     true_filter=true_filter,
                                     false_filter=false_filter)
        self.assertEqual(cond_filter1, cond_filter2)

    def test___eq__type_differ(self):
        base_filter = object()
        true_filter = object()
        false_filter = object()
        cond_filter1 = self._makeOne(base_filter,
                                     true_filter=true_filter,
                                     false_filter=false_filter)
        cond_filter2 = object()
        self.assertNotEqual(cond_filter1, cond_filter2)

    def test___ne__same_value(self):
        base_filter = object()
        true_filter = object()
        false_filter = object()
        cond_filter1 = self._makeOne(base_filter,
                                     true_filter=true_filter,
                                     false_filter=false_filter)
        cond_filter2 = self._makeOne(base_filter,
                                     true_filter=true_filter,
                                     false_filter=false_filter)
        comparison_val = (cond_filter1 != cond_filter2)
        self.assertFalse(comparison_val)

    def test_to_pb(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter1_pb = row_filter1.to_pb()

        row_filter2 = RowFilter(row_sample_filter=0.25)
        row_filter2_pb = row_filter2.to_pb()

        row_filter3 = RowFilter(cells_per_row_limit_filter=11)
        row_filter3_pb = row_filter3.to_pb()

        row_filter4 = self._makeOne(row_filter1, true_filter=row_filter2,
                                    false_filter=row_filter3)
        filter_pb = row_filter4.to_pb()

        expected_pb = data_pb2.RowFilter(
            condition=data_pb2.RowFilter.Condition(
                predicate_filter=row_filter1_pb,
                true_filter=row_filter2_pb,
                false_filter=row_filter3_pb,
            ),
        )
        self.assertEqual(filter_pb, expected_pb)

    def test_to_pb_true_only(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter1_pb = row_filter1.to_pb()

        row_filter2 = RowFilter(row_sample_filter=0.25)
        row_filter2_pb = row_filter2.to_pb()

        row_filter3 = self._makeOne(row_filter1, true_filter=row_filter2)
        filter_pb = row_filter3.to_pb()

        expected_pb = data_pb2.RowFilter(
            condition=data_pb2.RowFilter.Condition(
                predicate_filter=row_filter1_pb,
                true_filter=row_filter2_pb,
            ),
        )
        self.assertEqual(filter_pb, expected_pb)

    def test_to_pb_false_only(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable.row import RowFilter

        row_filter1 = RowFilter(strip_value_transformer=True)
        row_filter1_pb = row_filter1.to_pb()

        row_filter2 = RowFilter(row_sample_filter=0.25)
        row_filter2_pb = row_filter2.to_pb()

        row_filter3 = self._makeOne(row_filter1, false_filter=row_filter2)
        filter_pb = row_filter3.to_pb()

        expected_pb = data_pb2.RowFilter(
            condition=data_pb2.RowFilter.Condition(
                predicate_filter=row_filter1_pb,
                false_filter=row_filter2_pb,
            ),
        )
        self.assertEqual(filter_pb, expected_pb)


class _Client(object):

    data_stub = None


class _Table(object):

    def __init__(self, name, client=None, timeout_seconds=None):
        self.name = name
        self.client = client
        self.timeout_seconds = timeout_seconds
