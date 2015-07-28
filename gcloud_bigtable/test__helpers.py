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


class Test__pb_timestamp_to_datetime(unittest2.TestCase):

    def _callFUT(self, timestamp):
        from gcloud_bigtable._helpers import _pb_timestamp_to_datetime
        return _pb_timestamp_to_datetime(timestamp)

    def test_it(self):
        import datetime
        import pytz
        from gcloud_bigtable._generated.timestamp_pb2 import Timestamp

        # Epoch is midnight on January 1, 1970 ...
        dt_stamp = datetime.datetime(1970, month=1, day=1, hour=0,
                                     minute=1, second=1, microsecond=1234,
                                     tzinfo=pytz.utc)
        # ... so 1 minute and 1 second after is 61 seconds and 1234
        # microseconds is 1234000 nanoseconds.
        timestamp = Timestamp(seconds=61, nanos=1234000)
        self.assertEqual(self._callFUT(timestamp), dt_stamp)


class Test__require_pb_property(unittest2.TestCase):

    def _callFUT(self, message_pb, property_name, value):
        from gcloud_bigtable._helpers import _require_pb_property
        return _require_pb_property(message_pb, property_name, value)

    def test_it(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        serve_nodes = 119
        cluster_pb = data_pb2.Cluster(serve_nodes=serve_nodes)
        result = self._callFUT(cluster_pb, 'serve_nodes', serve_nodes)
        self.assertEqual(result, serve_nodes)

    def test_with_null_value_passed_in(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        serve_nodes = None
        actual_serve_nodes = 119
        cluster_pb = data_pb2.Cluster(serve_nodes=actual_serve_nodes)
        result = self._callFUT(cluster_pb, 'serve_nodes', serve_nodes)
        self.assertEqual(result, actual_serve_nodes)

    def test_with_value_unset_on_pb(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        serve_nodes = 119
        cluster_pb = data_pb2.Cluster()
        with self.assertRaises(ValueError):
            self._callFUT(cluster_pb, 'serve_nodes', serve_nodes)

    def test_with_values_disagreeing(self):
        from gcloud_bigtable._generated import (
            bigtable_cluster_data_pb2 as data_pb2)
        serve_nodes = 119
        other_serve_nodes = 1000
        self.assertNotEqual(serve_nodes, other_serve_nodes)
        cluster_pb = data_pb2.Cluster(serve_nodes=other_serve_nodes)
        with self.assertRaises(ValueError):
            self._callFUT(cluster_pb, 'serve_nodes', serve_nodes)


class Test__parse_pb_any_to_native(unittest2.TestCase):

    def _callFUT(self, any_val, expected_type=None):
        from gcloud_bigtable._helpers import _parse_pb_any_to_native
        return _parse_pb_any_to_native(any_val, expected_type=expected_type)

    def test_it(self):
        from gcloud_bigtable._generated import any_pb2
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        type_url = 'type.googleapis.com/' + data_pb2._CELL.full_name
        fake_type_url_map = {type_url: data_pb2.Cell}

        cell = data_pb2.Cell(
            timestamp_micros=0,
            value=b'foobar',
        )
        any_val = any_pb2.Any(
            type_url=type_url,
            value=cell.SerializeToString(),
        )
        with _Monkey(MUT, _TYPE_URL_MAP=fake_type_url_map):
            result = self._callFUT(any_val)

        self.assertEqual(result, cell)

    def test_unknown_type_url(self):
        from gcloud_bigtable._generated import any_pb2
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        fake_type_url_map = {}
        any_val = any_pb2.Any()
        with _Monkey(MUT, _TYPE_URL_MAP=fake_type_url_map):
            with self.assertRaises(KeyError):
                self._callFUT(any_val)

    def test_disagreeing_type_url(self):
        from gcloud_bigtable._generated import any_pb2
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        type_url1 = 'foo'
        type_url2 = 'bar'
        fake_type_url_map = {type_url1: None}
        any_val = any_pb2.Any(type_url=type_url2)
        with _Monkey(MUT, _TYPE_URL_MAP=fake_type_url_map):
            with self.assertRaises(ValueError):
                self._callFUT(any_val, expected_type=type_url1)


class Test__timedelta_to_duration_pb(unittest2.TestCase):

    def _callFUT(self, timedelta_val):
        from gcloud_bigtable._helpers import _timedelta_to_duration_pb
        return _timedelta_to_duration_pb(timedelta_val)

    def test_it(self):
        import datetime
        from gcloud_bigtable._generated import duration_pb2

        seconds = microseconds = 1
        timedelta_val = datetime.timedelta(seconds=seconds,
                                           microseconds=microseconds)
        result = self._callFUT(timedelta_val)
        self.assertTrue(isinstance(result, duration_pb2.Duration))
        self.assertEqual(result.seconds, seconds)
        self.assertEqual(result.nanos, 1000 * microseconds)

    def test_with_negative_microseconds(self):
        import datetime
        from gcloud_bigtable._generated import duration_pb2

        seconds = 1
        microseconds = -5
        timedelta_val = datetime.timedelta(seconds=seconds,
                                           microseconds=microseconds)
        result = self._callFUT(timedelta_val)
        self.assertTrue(isinstance(result, duration_pb2.Duration))
        self.assertEqual(result.seconds, seconds - 1)
        self.assertEqual(result.nanos, 10**9 + 1000 * microseconds)

    def test_with_negative_seconds(self):
        import datetime
        from gcloud_bigtable._generated import duration_pb2

        seconds = -1
        microseconds = 5
        timedelta_val = datetime.timedelta(seconds=seconds,
                                           microseconds=microseconds)
        result = self._callFUT(timedelta_val)
        self.assertTrue(isinstance(result, duration_pb2.Duration))
        self.assertEqual(result.seconds, seconds + 1)
        self.assertEqual(result.nanos, -(10**9 - 1000 * microseconds))
