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

*****************
Low-level Methods
*****************

The `ListClusters`_, `UndeleteCluster`_, and `ListZones`_ methods
have been implemented on the
:class:`ClusterConnection <gcloud_bigtable.cluster_connection.ClusterConnection>`
class, but not on the :class:`Cluster <gcloud_bigtable.cluster.Cluster>`
convenience class.

For now, you can access a cluster connection as a protected attribute of
a :class:`Cluster <gcloud_bigtable.cluster.Cluster>`:

.. code:: python

    cluster_connection = cluster._cluster_conn

    # Returns a
    # (gcloud_bigtable._generated.bigtable_cluster_service_messages_pb2.
    #  ListClustersResponse)
    list_of_clusters = cluster_connection.list_clusters(project_id)

    # Returns a
    # gcloud_bigtable._generated.operations_pb2.Operation
    long_running_operation = cluster_connection.undelete_cluster(
        project_id, zone, cluster_id)

If you don't know which zone you want the cluster in, create a
:class:`Cluster <gcloud_bigtable.cluster.Cluster>` without one and then
make a `ListZones`_ request to choose one:

.. code:: python

    cluster = Cluster(project_id, None, cluster_id)
    cluster_connection = cluster._cluster_conn

    # Returns a
    # (gcloud_bigtable._generated.bigtable_cluster_service_messages_pb2.
    #  ListZonesResponse)
    list_zones_response = cluster_connection.list_zones(
        cluster.cluster_id)
    zone_choices = [zone.display_name
                    for zone in list_zones_response.zones]

    # Found the best choice! The 4th zone.
    cluster.zone = zone_choices[3]

Low-level Responses
-------------------

These are low-level because ``list_of_clusters`` will be a
`ListClustersResponse`_, ``long_running_operation`` will be a
long-running `Operation`_ and ``list_zones_response`` will be a
`ListZonesResponse`_.

Documented Modules
~~~~~~~~~~~~~~~~~~

.. toctree::
   :maxdepth: 2

   base-connection
   data-connection
   table-connection
   cluster-connection
   cluster
   client

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
.. _ListClusters: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L44-L46
.. _UndeleteCluster: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L126-L128
.. _ListZones: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service.proto#L33-L35
.. _ListClustersResponse: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service_messages.proto#L56-L62
.. _Operation: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/longrunning/operations.proto#L73-L102
.. _ListZonesResponse: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/admin/cluster/v1/bigtable_cluster_service_messages.proto#L36-L39
