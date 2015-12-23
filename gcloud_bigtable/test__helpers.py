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


class TestMetadataTransformer(unittest2.TestCase):

    def _getTargetClass(self):
        from gcloud_bigtable._helpers import MetadataTransformer
        return MetadataTransformer

    def _makeOne(self, *args, **kwargs):
        return self._getTargetClass()(*args, **kwargs)

    def test_constructor(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client
        from gcloud_bigtable.client import DATA_SCOPE

        scoped_creds = object()
        credentials = _MockWithAttachedMethods(scoped_creds)
        project = 'PROJECT'
        user_agent = 'USER_AGENT'
        client = Client(project=project, credentials=credentials,
                        user_agent=user_agent)
        transformer = self._makeOne(client)
        self.assertTrue(transformer._credentials is scoped_creds)
        self.assertEqual(transformer._user_agent, user_agent)
        self.assertEqual(credentials._called, [
            ('create_scoped', ([DATA_SCOPE],), {}),
        ])

    def test___call__(self):
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable.client import Client
        from gcloud_bigtable.client import DATA_SCOPE
        from gcloud_bigtable.client import DEFAULT_USER_AGENT

        access_token_expected = 'FOOBARBAZ'

        class _ReturnVal(object):
            access_token = access_token_expected

        scoped_creds = _MockWithAttachedMethods(_ReturnVal)
        credentials = _MockWithAttachedMethods(scoped_creds)
        project = 'PROJECT'
        client = Client(project=project, credentials=credentials)

        transformer = self._makeOne(client)
        result = transformer(None)
        self.assertEqual(
            result,
            [
                ('Authorization', 'Bearer ' + access_token_expected),
                ('User-agent', DEFAULT_USER_AGENT),
            ])
        self.assertEqual(credentials._called, [
            ('create_scoped', ([DATA_SCOPE],), {}),
        ])
        self.assertEqual(scoped_creds._called, [('get_access_token', (), {})])


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


class Test__duration_pb_to_timedelta(unittest2.TestCase):

    def _callFUT(self, duration_pb):
        from gcloud_bigtable._helpers import _duration_pb_to_timedelta
        return _duration_pb_to_timedelta(duration_pb)

    def test_it(self):
        import datetime
        from gcloud_bigtable._generated import duration_pb2

        seconds = microseconds = 1
        duration_pb = duration_pb2.Duration(seconds=seconds,
                                            nanos=1000 * microseconds)
        timedelta_val = datetime.timedelta(seconds=seconds,
                                           microseconds=microseconds)
        result = self._callFUT(duration_pb)
        self.assertTrue(isinstance(result, datetime.timedelta))
        self.assertEqual(result, timedelta_val)


class Test__timestamp_to_microseconds(unittest2.TestCase):

    def _callFUT(self, timestamp, granularity=1000):
        from gcloud_bigtable._helpers import _timestamp_to_microseconds
        return _timestamp_to_microseconds(timestamp, granularity=granularity)

    def test_default_granularity(self):
        import datetime
        from gcloud_bigtable import _helpers as MUT

        microseconds = 898294371
        millis_granularity = microseconds - (microseconds % 1000)
        timestamp = MUT.EPOCH + datetime.timedelta(microseconds=microseconds)
        self.assertEqual(millis_granularity, self._callFUT(timestamp))

    def test_no_granularity(self):
        import datetime
        from gcloud_bigtable import _helpers as MUT

        microseconds = 11122205067
        timestamp = MUT.EPOCH + datetime.timedelta(microseconds=microseconds)
        self.assertEqual(microseconds, self._callFUT(timestamp, granularity=1))

    def test_non_utc_timestamp(self):
        import datetime

        epoch_no_tz = datetime.datetime.utcfromtimestamp(0)
        with self.assertRaises(TypeError):
            self._callFUT(epoch_no_tz)

    def test_non_datetime_timestamp(self):
        timestamp = object()  # Not a datetime object.
        with self.assertRaises(TypeError):
            self._callFUT(timestamp)


class Test__microseconds_to_timestamp(unittest2.TestCase):

    def _callFUT(self, microseconds):
        from gcloud_bigtable._helpers import _microseconds_to_timestamp
        return _microseconds_to_timestamp(microseconds)

    def test_it(self):
        import datetime
        from gcloud_bigtable import _helpers as MUT

        microseconds = 123456
        timestamp = MUT.EPOCH + datetime.timedelta(microseconds=microseconds)
        self.assertEqual(timestamp, self._callFUT(microseconds))


class Test__set_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable._helpers import _set_certs
        return _set_certs()

    def test_it(self):
        import tempfile
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        self.assertTrue(MUT.AuthInfo.ROOT_CERTIFICATES is None)

        filename = tempfile.mktemp()
        contents = b'FOOBARBAZ'
        with open(filename, 'wb') as file_obj:
            file_obj.write(contents)
        with _Monkey(MUT, SSL_CERT_FILE=filename):
            self._callFUT()

        self.assertEqual(MUT.AuthInfo.ROOT_CERTIFICATES, contents)
        # Reset to `None` value checked above.
        MUT.AuthInfo.ROOT_CERTIFICATES = None


class Test_set_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable._helpers import set_certs
        return set_certs()

    def test_call_private(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        call_count = [0]

        def mock_set_certs():
            call_count[0] += 1

        class _AuthInfo(object):
            ROOT_CERTIFICATES = None

        with _Monkey(MUT, AuthInfo=_AuthInfo,
                     _set_certs=mock_set_certs):
            self._callFUT()

        self.assertEqual(call_count, [1])

    def test_do_nothing(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        call_count = [0]

        def mock_set_certs():
            call_count[0] += 1

        # Make sure the fake method gets called by **someone** to make
        # tox -e cover happy.
        mock_set_certs()
        self.assertEqual(call_count, [1])

        class _AuthInfo(object):
            ROOT_CERTIFICATES = object()

        with _Monkey(MUT, AuthInfo=_AuthInfo,
                     _set_certs=mock_set_certs):
            self._callFUT()

        self.assertEqual(call_count, [1])


class Test_get_certs(unittest2.TestCase):

    def _callFUT(self):
        from gcloud_bigtable._helpers import get_certs
        return get_certs()

    def test_it(self):
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        call_kwargs = []
        return_val = object()

        def mock_set_certs(**kwargs):
            call_kwargs.append(kwargs)

        class _AuthInfo(object):
            ROOT_CERTIFICATES = return_val

        with _Monkey(MUT, AuthInfo=_AuthInfo,
                     set_certs=mock_set_certs):
            result = self._callFUT()

        self.assertEqual(call_kwargs, [{'reset': False}])
        self.assertTrue(result is return_val)


class Test_make_stub(unittest2.TestCase):

    def _callFUT(self, credentials, stub_factory, host, port):
        from gcloud_bigtable._helpers import make_stub
        return make_stub(credentials, stub_factory, host, port)

    def test_it(self):
        from gcloud_bigtable._testing import _MockCalled
        from gcloud_bigtable._testing import _MockWithAttachedMethods
        from gcloud_bigtable._testing import _Monkey
        from gcloud_bigtable import _helpers as MUT

        mock_result = object()
        custom_factory = _MockCalled(mock_result)
        transformed = object()
        transformer = _MockCalled(transformed)

        client_creds = object()
        channel = object()
        implementations_mod = _MockWithAttachedMethods(client_creds, channel)

        host = 'HOST'
        port = 1025
        certs = 'FOOBAR'
        client = _MockWithAttachedMethods()
        with _Monkey(MUT, get_certs=lambda: certs,
                     implementations=implementations_mod,
                     MetadataTransformer=transformer):
            result = self._callFUT(client, custom_factory, host, port)

        self.assertTrue(result is mock_result)
        custom_factory.check_called(
            self,
            [(channel,)],
            [{
                'metadata_transformer': transformed,
            }],
        )
        transformer.check_called(self, [(client,)])
        self.assertEqual(client._called, [])
        # Check what was called on the module.
        self.assertEqual(implementations_mod._called, [
            ('ssl_client_credentials', (certs,),
             {'private_key': None, 'certificate_chain': None}),
            ('secure_channel', (host, port, client_creds), {}),
        ])


class Test__parse_family_pb(unittest2.TestCase):

    def _callFUT(self, family_pb):
        from gcloud_bigtable._helpers import _parse_family_pb
        return _parse_family_pb(family_pb)

    def test_it(self):
        from gcloud_bigtable._generated import bigtable_data_pb2 as data_pb2
        from gcloud_bigtable._helpers import _microseconds_to_timestamp

        COL_FAM1 = u'col-fam-id'
        COL_NAME1 = b'col-name1'
        COL_NAME2 = b'col-name2'
        CELL_VAL1 = b'cell-val'
        CELL_VAL2 = b'cell-val-newer'
        CELL_VAL3 = b'altcol-cell-val'

        microseconds = 5554441037
        timestamp = _microseconds_to_timestamp(microseconds)
        expected_dict = {
            COL_NAME1: [
                (CELL_VAL1, timestamp),
                (CELL_VAL2, timestamp),
            ],
            COL_NAME2: [
                (CELL_VAL3, timestamp),
            ],
        }
        expected_output = (COL_FAM1, expected_dict)
        sample_input = data_pb2.Family(
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
        )
        self.assertEqual(expected_output, self._callFUT(sample_input))


class Test__to_bytes(unittest2.TestCase):

    def _callFUT(self, *args, **kwargs):
        from gcloud_bigtable._helpers import _to_bytes
        return _to_bytes(*args, **kwargs)

    def test_with_bytes(self):
        value = b'bytes-val'
        self.assertEqual(self._callFUT(value), value)

    def test_with_unicode(self):
        value = u'string-val'
        encoded_value = b'string-val'
        self.assertEqual(self._callFUT(value), encoded_value)

    def test_unicode_non_ascii(self):
        value = u'\u2013'  # Long hyphen
        encoded_value = b'\xe2\x80\x93'
        self.assertRaises(UnicodeEncodeError, self._callFUT, value)
        self.assertEqual(self._callFUT(value, encoding='utf-8'),
                         encoded_value)

    def test_with_nonstring_type(self):
        value = object()
        self.assertRaises(TypeError, self._callFUT, value)
