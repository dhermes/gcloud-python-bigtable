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


class TestMutation(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable.mutation import Mutation
        return Mutation

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        mutation = self._makeOne()
        self.assertEqual(mutation._pb_mutations, [])

    def _set_method_helper(self, column_name=b'column_name',
                           column_name_bytes=None, timestamp=None,
                           timestamp_micros=-1):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2

        mutation = self._makeOne()
        column_family_id = u'column_family_id'
        value = b'foobar'
        mutation.set(column_family_id, column_name,
                     value, timestamp=timestamp)

        expected_pb = data_pb2.Mutation(
            set_cell=data_pb2.Mutation.SetCell(
                family_name=column_family_id,
                column_qualifier=column_name_bytes or column_name,
                timestamp_micros=timestamp_micros,
                value=value,
            ),
        )
        self.assertEqual(mutation._pb_mutations, [expected_pb])

    def test_set(self):
        column_name = b'column_name'
        self._set_method_helper(column_name=column_name)

    def test_set_with_string_column_name(self):
        column_name = u'column_name'
        column_name_bytes = b'column_name'
        self._set_method_helper(column_name=column_name,
                                column_name_bytes=column_name_bytes)

    def test_set_with_non_bytes_value(self):
        mutation = self._makeOne()
        value = object()  # Not bytes
        with self.assertRaises(TypeError):
            mutation.set(None, None, value)

    def test_set_with_non_null_timestamp(self):
        import datetime
        from gcloud_bigtable import mutation as MUT

        microseconds = 898294371
        timestamp = MUT._EPOCH + datetime.timedelta(microseconds=microseconds)
        self._set_method_helper(timestamp=timestamp,
                                timestamp_micros=microseconds)

    def test_set_with_non_utc_timestamp(self):
        import datetime

        microseconds = 0
        epoch_no_tz = datetime.datetime.utcfromtimestamp(0)
        with self.assertRaises(TypeError):
            self._set_method_helper(timestamp=epoch_no_tz,
                                    timestamp_micros=microseconds)

    def test_set_with_non_datetime_timestamp(self):
        timestamp = object()  # Not a datetime object.
        with self.assertRaises(TypeError):
            self._set_method_helper(timestamp=timestamp)
