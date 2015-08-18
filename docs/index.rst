Google Cloud Bigtable: Python
=============================

This library is an alpha implementation of `Google Cloud Bigtable`_
and is closely related to `gcloud-python`_.

API requests are sent to the Google Cloud Bigtable API via RPC over HTTP/2.
In order to support this, we'll rely on `gRPC`_. We are working with the gRPC
team to rapidly make the install story more user-friendly.

Head on over to the `Getting Started`_ page to learn more
about using this API. If you have install questions, check out the
project's `README`_.

.. toctree::
   :maxdepth: 2
   :hidden:

   all-apis
   constants
   client
   cluster
   table
   column-family
   row
   row-data

Indices and tables
~~~~~~~~~~~~~~~~~~

* :ref:`genindex`
* :ref:`modindex`

.. _Google Cloud Bigtable: https://cloud.google.com/bigtable/docs/
.. _gcloud-python: http://gcloud-python.readthedocs.org/en/latest/
.. _gRPC: http://www.grpc.io/
.. _README: https://github.com/dhermes/gcloud-python-bigtable/blob/master/README.md
.. _Getting Started: all-apis.html
