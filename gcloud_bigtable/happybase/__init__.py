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

"""Google Cloud Bigtable HappyBase package.

Intended to emulate the HappyBase library using Google Cloud BigTable
as the backing store.
"""

from gcloud_bigtable.happybase.batch import Batch
from gcloud_bigtable.happybase.connection import Connection
from gcloud_bigtable.happybase.connection import DEFAULT_HOST
from gcloud_bigtable.happybase.connection import DEFAULT_PORT
from gcloud_bigtable.happybase.pool import ConnectionPool
from gcloud_bigtable.happybase.pool import NoConnectionsAvailable


# Types that have yet to be implemented
class Table(object):
    """Unimplemented Table stub."""


# Values from HappyBase that we don't reproduce / are not relevant.
__version__ = None
