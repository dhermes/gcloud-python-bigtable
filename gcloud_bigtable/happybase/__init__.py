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

Intended to emulate the HappyBase library using Google Cloud Bigtable
as the backing store.

Some concepts from HBase/Thrift do not map directly to the Cloud
Bigtable API. As a result, the following instance methods and functions
could not be implemented:

* :meth:`.Connection.enable_table` - no concept of enabled/disabled
* :meth:`.Connection.disable_table` - no concept of enabled/disabled
* :meth:`.Connection.is_table_enabled` - no concept of enabled/disabled
* :meth:`.Connection.compact_table` - table storage is opaque to user
* :func:`make_row() <gcloud_bigtable.happybase.table.make_row>` - helper
  needed for Thrift library
* :func:`make_ordered_row() <gcloud_bigtable.happybase.table.make_ordered_row>`
  - helper needed for Thrift library
* :meth:`Table.regions() <gcloud_bigtable.happybase.table.Table.regions>`
  - tables in Cloud Bigtable do not expose internal storage details
* :meth:`Table.counter_set() \
      <gcloud_bigtable.happybase.table.Table.counter_set>` - method can't
  be atomic, so we disable it

This also means that calling :meth:`.Connection.delete_table` with
``disable=True`` can't be supported.

In addition, the many of the constants from :mod:`.connection` are specific
to HBase and are defined as :data:`None` in our module:

* ``COMPAT_MODES``
* ``THRIFT_TRANSPORTS``
* ``THRIFT_PROTOCOLS``
* ``DEFAULT_HOST``
* ``DEFAULT_PORT``
* ``DEFAULT_TRANSPORT``
* ``DEFAULT_COMPAT``
* ``DEFAULT_PROTOCOL``

Two of these ``DEFAULT_HOST`` and ``DEFAULT_PORT``, are even imported in
the main ``happybase`` package.

The :class:`.Connection` constructor **disables** the use of several arguments
and will a :class:`ValueError <exceptions.ValueError>` if any of them are
passed in as keyword arguments. The arguments are:

- ``host``
- ``port``
- ``compat``
- ``transport``
- ``protocol``

In order to make :class:`.Connection` compatible with Cloud Bigtable, we
add a ``client`` keyword argument to allow user's to pass in their own
clients (which they can construct beforehand).

Any uses of the ``wal`` (Write Ahead Log) argument will result in a
:class:`ValueError <exceptions.ValueError>` as well. This includes
uses in:

* :class:`.Batch` constructor
* :meth:`.Batch.put`
* :meth:`.Batch.delete`
* :meth:`Table.put() <gcloud_bigtable.happybase.table.Table.put>`
* :meth:`Table.delete() <gcloud_bigtable.happybase.table.Table.delete>`
* :meth:`Table.batch() <gcloud_bigtable.happybase.table.Table.batch>` factory

Finally, we do not provide the ``util`` module. Though it is public in the
HappyBase library, it provides no core functionality.
"""

from gcloud_bigtable.happybase.batch import Batch
from gcloud_bigtable.happybase.connection import Connection
from gcloud_bigtable.happybase.connection import DEFAULT_HOST
from gcloud_bigtable.happybase.connection import DEFAULT_PORT
from gcloud_bigtable.happybase.pool import ConnectionPool
from gcloud_bigtable.happybase.pool import NoConnectionsAvailable
from gcloud_bigtable.happybase.table import Table


# Values from HappyBase that we don't reproduce / are not relevant.
__version__ = None
