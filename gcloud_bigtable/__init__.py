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

from __future__ import absolute_import
import os
import sys

import google
from google import protobuf

# Add _generated/google/ to google namespace package so that
# generated google.* modules are imported.
CURR_DIR = os.path.abspath(os.path.dirname(__file__))
GENERATED_DIR = os.path.join(CURR_DIR, '_generated', 'google')
PROTOBUF_DIR = os.path.join(GENERATED_DIR, 'protobuf')
google.__path__.append(GENERATED_DIR)
protobuf.__path__.append(PROTOBUF_DIR)

# Shadow the `gcloud_bigtable._generated.google` package with
# the already existing `google` package.
ALTERNATE_GOOGLE_KEY = __name__ + '._generated.google'
if ALTERNATE_GOOGLE_KEY in sys.modules:
    raise ImportError(ALTERNATE_GOOGLE_KEY, 'has already been imported')
sys.modules[ALTERNATE_GOOGLE_KEY] = google

# Shadow the `gcloud_bigtable._generated.google.protobuf` package with
# the already existing `google.protobuf` package.
ALTERNATE_PROTOBUF_KEY = ALTERNATE_GOOGLE_KEY + '.protobuf'
if ALTERNATE_PROTOBUF_KEY in sys.modules:
    raise ImportError(ALTERNATE_PROTOBUF_KEY, 'has already been imported')
sys.modules[ALTERNATE_PROTOBUF_KEY] = protobuf

# Clean-up unneeded variable names.
del CURR_DIR
del GENERATED_DIR
del PROTOBUF_DIR
del ALTERNATE_GOOGLE_KEY
del ALTERNATE_PROTOBUF_KEY
