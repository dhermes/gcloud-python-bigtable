Google Cloud Bigtable: Python
=============================

The `Cluster Admin API`_ has been fully implemented.

To use the API, the :class:`Cluster <gcloud_bigtable.cluster.Cluster>`
class defines a high-level interface:

.. code:: python

    from gcloud_bigtable.cluster import Cluster
    cluster = Cluster(project_id, zone, cluster_id)

********************
Create a new Cluster
********************

After creating the cluster object, make a `CreateCluster`_ API request:

.. code:: python

    display_name = 'My very own cluster'
    cluster.create(display_name)

If you would like more than the minimum number of nodes (``3``) in your cluster:

.. code:: python

    serve_nodes = 10
    cluster.create(display_name, serve_nodes=serve_nodes)

.. note::

    Currently ``serve_nodes`` will default to ``3`` if not passed to
    ``create`` and ``display_name`` is mandatory in ``create`` since it is
    mandatory in ``CreateCluster``. However, we could easily set

    .. code:: python

        display_name = cluster.cluster_id

    if ``cluster.create()`` were called with no arguments.

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

    cluster.update(display_name=display_name, serve_nodes=serve_nodes)

Both ``display_name`` and ``serve_nodes`` are optional arguments. If
neither are passed in, the method will do nothing.

.. note::

    This means that if you change them via:

.. code:: python

    cluster.display_name = 'Brand new display name'
    cluster.update()

then the ``update`` won't actually occur. (Though we are open to changing
this behavior. The current behavior is in place to avoid an HTTP/2 request
if one is not necessary. This behavior would not be possible if
``display_name`` and ``serve_nodes`` were read-only properties.)

**************************
Delete an existing Cluster
**************************

Make a `DeleteCluster`_ API request:

.. code:: python

    cluster.delete()

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
