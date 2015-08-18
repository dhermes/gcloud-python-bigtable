Table Admin API
===============

After creating a :class:`Cluster <gcloud_bigtable.cluster.Cluster>`, you can
interact with individual tables, groups of tables or column families within
a table.

List Tables
-----------

If you want a comprehensive list of all existing tables in a cluster, make a
`ListTables`_ API request with
:meth:`Cluster.list_tables() <gcloud_bigtable.cluster.Cluster.list_tables>`:

.. code:: python

    tables = cluster.list_tables()

Table Factory
-------------

To create a :class:`Table <gcloud_bigtable.table.Table>` object:

.. code:: python

    table = cluster.table(table_id)

Even if this :class:`Table <gcloud_bigtable.table.Table>` already
has been created with the API, you'll want this object to use as a
parent of a :class:`ColumnFamily <gcloud_bigtable.column_family.ColumnFamily>`
or :class:`Row <gcloud_bigtable.row.Row>`.

Create a new Table
------------------

After creating the table object, make a `CreateTable`_ API request
with :meth:`create() <gcloud_bigtable.table.Table.create>`:

.. code:: python

    table.create()

If you would to initially split the table into several tablets (Tablets are
similar to HBase regions):

.. code:: python

    table.create(initial_split_keys=['s1', 's2'])

Delete an existing Table
------------------------

Make a `DeleteTable`_ API request with
:meth:`delete() <gcloud_bigtable.table.Table.delete>`:

.. code:: python

    table.delete()

Rename an existing Table
------------------------

Though the `RenameTable`_ API request is listed in the service
definition, requests to that method return::

    BigtableTableService.RenameTable is not yet implemented

We have implemented :meth:`rename() <gcloud_bigtable.table.Table.rename>`
but it will not work unless the backend supports the method.

List Column Families in a Table
-------------------------------

Though there is no **official** method for retrieving `column families`_
associated with a table, the `GetTable`_ API method returns a
table object with the names of the column families.

To retrieve the list of column families use
:meth:`list_column_families() <gcloud_bigtable.table.Table.list_column_families>`:

.. code:: python

    column_families = table.list_column_families()

.. note::

    Unfortunately the garbage collection rules used to create each column family
    are not returned in the `GetTable`_ response.

Column Family Factory
---------------------

Factory

Create a new Column Family
--------------------------

`CreateColumnFamily`_

Update an existing Column Family
--------------------------------

`UpdateColumnFamily`_

Delete an existing Column Family
--------------------------------

`DeleteColumnFamily`_

.. _ListTables: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L40-L42
.. _CreateTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L35-L37
.. _DeleteTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L50-L52
.. _RenameTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L58-L58
.. _GetTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L45-L47
.. _CreateColumnFamily: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L61-L63
.. _UpdateColumnFamily: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L66-L68
.. _DeleteColumnFamily: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L71-L73
.. _column families: https://cloud.google.com/bigtable/docs/schema-design#column_families_and_column_qualifiers
