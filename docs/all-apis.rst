Getting Started
===============

To use the API, the :class:`Client <gcloud_bigtable.client.Client>`
class defines a high-level interface which handles authorization
and creating other objects:

.. code:: python

    from gcloud_bigtable.client import Client
    cluster = Client()

This will use the Google `Application Default Credentials`_ if
you don't pass any credentials of your own.

.. toctree::
   :maxdepth: 2
   :hidden:

   cluster-api

.. _Application Default Credentials: https://developers.google.com/identity/protocols/application-default-credentials
