Cluster Admin API
=================

After creating a :class:`Client <gcloud_bigtable.client.Client>`, you can
interact with individual clusters, groups of clusters or available
zones for a project.

List Clusters
-------------

If you want a comprehensive list of all existing clusters, make a
`ListClusters`_ API request with
:meth:`Client.list_clusters() <gcloud_bigtable.client.Client.list_clusters>`:

.. code:: python

    clusters = client.list_clusters()

List Zones
----------

If you aren't sure which ``zone`` to create a cluster in, find out
which zones your project has access to with a `ListZones`_ API request
with :meth:`Client.list_zones() <gcloud_bigtable.client.Client.list_zones>`:

.. code:: python

    zones = client.list_zones()

You can choose a :class:`string <str>` from among the result to pass to
the :class:`Cluster <gcloud_bigtable.cluster.Cluster>` constructor.

Cluster Factory
---------------

To create a :class:`Cluster <gcloud_bigtable.cluster.Cluster>` object:

.. code:: python

    cluster = client.cluster(zone, cluster_id,
                             display_name=display_name,
                             serve_nodes=3)

Both ``display_name`` and ``serve_nodes`` are optional. When not provided,
``display_name`` defaults to the ``cluster_id`` value and ``serve_nodes``
defaults to the minimum allowed: ``3``.

Even if this :class:`Cluster <gcloud_bigtable.cluster.Cluster>` already
has been created with the API, you'll want this object to use as a
parent of a :class:`Table <gcloud_bigtable.table.Table>` just as the
:class:`Client <gcloud_bigtable.client.Client>` is used as the parent of
a :class:`Cluster <gcloud_bigtable.cluster.Cluster>`.

Create a new Cluster
--------------------

After creating the cluster object, make a `CreateCluster`_ API request
with :meth:`create() <gcloud_bigtable.cluster.Cluster.create>`:

.. code:: python

    cluster.display_name = 'My very own cluster'
    cluster.create()

If you would like more than the minimum number of nodes (``3``) in your cluster:

.. code:: python

    cluster.serve_nodes = 10
    cluster.create()

Check on Current Operation
--------------------------

.. note::

    When modifying a cluster (via a `CreateCluster`_, `UpdateCluster`_ or
    `UndeleteCluster`_ request), the Bigtable API will return a long-running
    `Operation`_. This will be stored on the object after each of
    :meth:`create() <gcloud_bigtable.cluster.Cluster.create>`,
    :meth:`update() <gcloud_bigtable.cluster.Cluster.update>` and
    :meth:`undelete() <gcloud_bigtable.cluster.Cluster.undelete>` are called.

.. _Operation: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/longrunning/operations.proto#L73-L102

You can check if a long-running operation (for a
:meth:`create() <gcloud_bigtable.cluster.Cluster.create>`,
:meth:`update() <gcloud_bigtable.cluster.Cluster.update>` or
:meth:`undelete() <gcloud_bigtable.cluster.Cluster.undelete>`) has finished
by making a `GetOperation`_ request with
:meth:`operation_finished() <gcloud_bigtable.cluster.Cluster.operation_finished>`:

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

Get metadata for an existing Cluster
------------------------------------

After creating the cluster object, make a `GetCluster`_ API request
with :meth:`reload() <gcloud_bigtable.cluster.Cluster.reload>`:

.. code:: python

    cluster.reload()

This will load ``serve_nodes`` and ``display_name`` for the existing
``cluster`` in addition to the ``cluster_id``, ``zone`` and ``project_id``
already set on the :class:`Cluster <gcloud_bigtable.cluster.Cluster>` object.

Update an existing Cluster
--------------------------

After creating the cluster object, make an `UpdateCluster`_ API request
with :meth:`update() <gcloud_bigtable.cluster.Cluster.update>`:

.. code:: python

    client.display_name = 'New display_name'
    cluster.update()

Delete an existing Cluster
--------------------------

Make a `DeleteCluster`_ API request with
:meth:`delete() <gcloud_bigtable.cluster.Cluster.delete>`:

.. code:: python

    cluster.delete()

Undelete a deleted Cluster
--------------------------

Make an `UndeleteCluster`_ API request with
:meth:`undelete() <gcloud_bigtable.cluster.Cluster.undelete>`:

.. code:: python

    cluster.undelete()

Next Step
---------

Now we go down the hierarchy from
:class:`Cluster <gcloud_bigtable.cluster.Cluster>` to a
:class:`Table <gcloud_bigtable.table.Table>`.

Head next to learn about the `Table Admin API`_.

.. _Cluster Admin API: https://cloud.google.com/bigtable/docs/creating-cluster
.. _CreateCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L66-L68
.. _GetCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L38-L40
.. _UpdateCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L93-L95
.. _DeleteCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L109-L111
.. _ListZones: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L33-L35
.. _ListClusters: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L44-L46
.. _GetOperation: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/bfe4138f04bf3383a558152e4333112cdd13d5b0/bigtable-protos/src/main/proto/google/longrunning/operations.proto#L43-L45
.. _UndeleteCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L126-L128
.. _Table Admin API: table-api.html
