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

"""Google Cloud BigTable API package."""


import json
import six


DEBUG = False


def _print_func(value):
    """Print function which does nothing unless in DEBUG mode.

    :type value: object
    :param value: The value to print.
    """
    if DEBUG:
        six.print_(value)


def _print_error(headers, content):
    """Pretty prints headers and body from an error.

    :type headers: :class:`httplib2.Response`
    :param headers: Response headers from an error.

    :type content: string
    :param content: Error response body.
    """
    _print_func('RESPONSE HEADERS:')
    _print_func(json.dumps(headers, indent=2, sort_keys=True))
    _print_func('-' * 60)
    _print_func('RESPONSE BODY:')
    _print_func(json.dumps(json.loads(content), indent=2, sort_keys=True))
