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

To create a
:class:`ColumnFamily <gcloud_bigtable.column_family.ColumnFamily>` object:

.. code:: python

    column_family = table.column_family(column_family_id)

There is no real reason to use this factory unless you intend to
create or delete a column family.

In addition, you can specify an optional ``gc_rule`` (a
:class:`GarbageCollectionRule <gcloud_bigtable.column_family.GarbageCollectionRule>`
or similar):

.. code:: python

    column_family = table.column_family(column_family_id,
                                        gc_rule=gc_rule)

This rule helps the backend determine when and how to clean up old cells
in the column family.

See the `Column Families doc`_ for more information about
:class:`GarbageCollectionRule <gcloud_bigtable.column_family.GarbageCollectionRule>`
and related classes.

Create a new Column Family
--------------------------

After creating the column family object, make a `CreateColumnFamily`_ API
request with
:meth:`ColumnFamily.create() <gcloud_bigtable.column_family.ColumnFamily.create>`

.. code:: python

    column_family.create()

Delete an existing Column Family
--------------------------------

Make a `DeleteColumnFamily`_ API request with
:meth:`ColumnFamily.delete() <gcloud_bigtable.column_family.ColumnFamily.delete>`

.. code:: python

    column_family.delete()

Update an existing Column Family
--------------------------------

Though the `UpdateColumnFamily`_ API request is listed in the service
definition, requests to that method return::

    BigtableTableService.UpdateColumnFamily is not yet implemented

We have implemented
:meth:`ColumnFamily.update() <gcloud_bigtable.column_family.ColumnFamily.update>`
but it will not work unless the backend supports the method.

Next Step
---------

Now we go down the final step of the hierarchy from
:class:`Table <gcloud_bigtable.table.Table>` to
:class:`Row <gcloud_bigtable.row.Row>` as well as streaming
data directly via a :class:`Table <gcloud_bigtable.table.Table>`.

Head next to learn about the `Data API`_.

.. _ListTables: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L40-L42
.. _CreateTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L35-L37
.. _DeleteTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L50-L52
.. _RenameTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L58-L58
.. _GetTable: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L45-L47
.. _CreateColumnFamily: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L61-L63
.. _UpdateColumnFamily: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L66-L68
.. _DeleteColumnFamily: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/f4d922bb950f1584b30f9928e84d042ad59f5658/bigtable-protos/src/main/proto/google/bigtable/admin/table/v1/bigtable_table_service.proto#L71-L73
.. _column families: https://cloud.google.com/bigtable/docs/schema-design#column_families_and_column_qualifiers
.. _Column Families doc: column-family.html
.. _Data API: data-api.html
