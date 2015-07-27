Google Cloud Bigtable: Python
=============================

To use the API, the :class:`Client <gcloud_bigtable.client.Client>`
class defines a high-level interface which handles authorization
and creating other objects:

.. code:: python

    from gcloud_bigtable.client import Client
    cluster = Client(credentials)

The `Cluster Admin API`_ has been fully implemented. Create a
:class:`Cluster <gcloud_bigtable.cluster.Cluster>` to get
a high-level interface to cluster management:

.. code:: python

    cluster = client.cluster(zone, cluster_id)

*************
List Clusters
*************

If you want a comprehensive list of all existing clusters,
make a `ListClusters`_ request:

.. code:: python

    clusters = client.list_clusters()

This will return a list of
:class:`Cluster <gcloud_bigtable.cluster.Cluster>` s.

**********
List Zones
**********

If you aren't sure which ``zone`` to create a cluster in, find out
which zones your project has access to with a `ListZones`_ request:

.. code:: python

    zones = client.list_clusters()

This will return a list of ``string`` s.

********************
Create a new Cluster
********************

After creating the cluster object, make a `CreateCluster`_ API request:

.. code:: python

    cluster.display_name = 'My very own cluster'
    cluster.create()

If you would like more than the minimum number of nodes (``3``) in your cluster:

.. code:: python

    cluster.serve_nodes = 10
    cluster.create()

.. note::

    When modifying a cluster (via a `CreateCluster`_, `UpdateCluster`_ or
    `UndeleteCluster`_ request), the Bigtable API will return a long-running
    `Operation`_. This will be stored on the object after each of
    :meth:`create() <gcloud_bigtable.cluster.Cluster.create>`,
    :meth:`update() <gcloud_bigtable.cluster.Cluster.update>` and
    :meth:`undelete() <gcloud_bigtable.cluster.Cluster.undelete>` are called.

.. _Operation: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/longrunning/operations.proto#L73-L102

**************************
Check on Current Operation
**************************

You can check if a long-running operation (for a
:meth:`create() <gcloud_bigtable.cluster.Cluster.create>`,
:meth:`update() <gcloud_bigtable.cluster.Cluster.update>` or
:meth:`undelete() <gcloud_bigtable.cluster.Cluster.undelete>`) has finished
by making a `GetOperation`_ request:

.. code:: python

    >>> cluster.operation_finished()
    True

.. note::

    The operation data is stored in protected fields on the
    :class:`Cluster <gcloud_bigtable.cluster.Cluster>`:
    ``_operation_type``, ``_operation_id`` and ``_operation_begin``.
    If these are unset, then
    :meth:`operation_finished() <gcloud_bigtable.cluster.Cluster.operation_finished>`
    will fail. Also, these will be removed after a long-running operation
    has completed (checked via this method). We could easily surface these
    properties publicly, but it's unclear if end-users would need them.

************************************
Get metadata for an existing Cluster
************************************

After creating the cluster object, make a `GetCluster`_ API request:

.. code:: python

    cluster.reload()

This will load ``serve_nodes`` and ``display_name`` for the existing
``cluster`` in addition to the ``cluster_id``, ``zone`` and ``project_id``
already set on the :class:`Cluster <gcloud_bigtable.cluster.Cluster>` object.

**************************
Update an existing Cluster
**************************

After creating the cluster object, make an `UpdateCluster`_ API request:

.. code:: python

    client.display_name = 'New display_name'
    cluster.update()

**************************
Delete an existing Cluster
**************************

Make a `DeleteCluster`_ API request:

.. code:: python

    cluster.delete()

**************************
Undelete a deleted Cluster
**************************

Make a `UndeleteCluster`_ API request:

.. code:: python

    cluster.undelete()

Documented Modules
~~~~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   base-connection
   data-connection
   table-connection
   client
   cluster

Indices and tables
~~~~~~~~~~~~~~~~~~

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Cluster Admin API: https://cloud.google.com/bigtable/docs/creating-cluster
.. _CreateCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L66-L68
.. _GetCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L38-L40
.. _UpdateCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L93-L95
.. _DeleteCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L109-L111
.. _ListZones: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L33-L35
.. _ListClusters: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L44-L46
.. _GetOperation: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/bfe4138f04bf3383a558152e4333112cdd13d5b0/bigtable-protos/src/main/proto/google/longrunning/operations.proto#L43-L45
.. _UndeleteCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L126-L128
