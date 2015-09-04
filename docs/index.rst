.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting Started

   client-intro
   client

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Cluster Admin

   cluster-api
   cluster

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Table Admin

   table-api
   table
   column-family

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Data API

   data-api
   row
   row-data

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: HappyBase Compatibility Sub-Package

   happybase-connection

Google Cloud Bigtable: Python
=============================

This library is an alpha implementation of `Google Cloud Bigtable`_
and is closely related to `gcloud-python`_.

API requests are sent to the Google Cloud Bigtable API via RPC over HTTP/2.
In order to support this, we'll rely on `gRPC`_. We are working with the gRPC
team to rapidly make the install story more user-friendly.

Get started by learning about the
:class:`Client <gcloud_bigtable.client.Client>` on the `Base for Everything`_
page. If you have install questions, check out the project's `README`_.

In the hierarchy of API concepts

* a :class:`Client <gcloud_bigtable.client.Client>` owns a
  :class:`Cluster <gcloud_bigtable.cluster.Cluster>`
* a :class:`Cluster <gcloud_bigtable.cluster.Cluster>` owns a
  :class:`Table <gcloud_bigtable.table.Table>`
* a :class:`Table <gcloud_bigtable.table.Table>` owns a
  :class:`ColumnFamily <gcloud_bigtable.column_family.ColumnFamily>`
* a :class:`Table <gcloud_bigtable.table.Table>` owns a
  :class:`Row <gcloud_bigtable.row.Row>`
  (and all the cells in the row)

Indices and tables
~~~~~~~~~~~~~~~~~~

* :ref:`genindex`
* :ref:`modindex`

.. _Google Cloud Bigtable: https://cloud.google.com/bigtable/docs/
.. _gcloud-python: http://gcloud-python.readthedocs.org/en/latest/
.. _gRPC: http://www.grpc.io/
.. _README: https://github.com/dhermes/gcloud-python-bigtable/blob/master/README.md
.. _Base for Everything: client-intro.html
