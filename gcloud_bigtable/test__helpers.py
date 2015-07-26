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
