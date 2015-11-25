# Google Cloud Bigtable Python Library (Alpha)

[![Documentation Status](https://readthedocs.org/projects/gcloud-python-bigtable/badge/?version=latest)](https://readthedocs.org/projects/gcloud-python-bigtable/?badge=latest)

### An extension to [`gcloud-python`][1]

This library supports RPC requests to the Google Cloud Bigtable API over
HTTP/2. In order to support this, we'll rely on [`grpc`][2]. Unfortunately,
the install story of gRPC is still developing. We are working with the
gRPC team to rapidly make the install story more user-friendly.

The Cloud Bigtable API is supported via JSON over HTTP/1.1 (see the
[`gcloud` CLI][4] use of the API). However, features of HTTP/2 such
as streaming are [used][5] by the Bigtable API and are not possible
to support via HTTP/1.1.

## Installing gRPC

Before you can install the Python library (`grpcio`) you'll need the
gRPC core library on your system. To install this on Debian Linux:

```bash
$ apt-get install libgrpc-dev
```

or on Mac OS X:

```bash
$ brew tap grpc/grpc
$ brew install grpc
```

Once you've installed the system library, install the Python
library via:

```bash
$ pip install --upgrade grpcio
```

Finally, you can install this library via

```bash
pip install -e git+https://github.com/dhermes/gcloud-python-bigtable#egg=gcloud-bigtable
```

## Authorization

You can make requests with your own Google account by
using the [`gcloud` CLI tool][8]. You can create an access token via

```bash
gcloud login
```

and then from there, the token created will be picked up automatically
when you create an object which requires authentication:

```python
from gcloud_bigtable.client import Client
client = Client()
```

If instead you'd like to use a service account, you can set an
environment variable to the path containing the service account
credentials JSON file:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
```

If you are **familiar** with the [`oauth2client`][9] library,
you can create a `credentials` object and pass it directly:

```python
from gcloud_bigtable.client import Client
client = Client(credentials=credentials)
```

## Enabling the Bigtable API

1.  Visit [Google Cloud Console][14]
1.  Either create a new project or visit an existing one
1.  In the project, click **"APIs & auth > APIs"**. The URI
    should be of the form

    ```
    https://console.developers.google.com/project/{project-id}/apiui/apis/library
    ```

1.  On this page, search for **bigtable**, and click both `Cloud Bigtable API`
    and `Cloud Bigtable Table Admin API`.
1.  For each API, click "Enable API" (if not already enabled)

## Getting a Service Account Keyfile

1.  Visit [Google Cloud Console][14]
1.  Either create a new project or visit an existing one
1.  In the project, click **"APIs & auth > Credentials"**. The URI
    should be of the form

    ```
    https://console.developers.google.com/project/{project-id}/apiui/credential
    ```

1.  On this page, click "Create new Client ID", select "Service account" as
    your "Application type" and then download the JSON key provided.

After downloading, you can use this file as your
`GOOGLE_APPLICATION_CREDENTIALS`.

## Creating a Cluster in the UI

1.  Visit [Google Cloud Console][14]
1.  Either create a new project or visit an existing one
1.  In the project, click **"Storage > Cloud Bigtable"**. The URI
    should be of the form

    ```
    https://console.developers.google.com/project/{project-id}/bigtable/clusters
    ```

1.  On this page, click **Create a cluster** and take note of the "Cluster ID"
    and "Zone" you use when creating it.

## Error Messages

Unfortunately, the gRPC Python library does not surface
exceptions returned from the API. An [issue][10] has been
filed with the gRPC team about this problem.

## Development

See [`CONTRIBUTING.md`][3] for instructions on development.

[1]: https://github.com/GoogleCloudPlatform/gcloud-python
[2]: https://www.grpc.io/
[3]: https://github.com/dhermes/gcloud-python-bigtable/blob/master/CONTRIBUTING.md
[4]: https://cloud.google.com/sdk/gcloud/reference/alpha/bigtable/clusters/list
[5]: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/v1/bigtable_service.proto#L36-L46
[6]: http://brew.sh/
[7]: https://github.com/Homebrew/linuxbrew#install-linuxbrew-tldr
[8]: https://cloud.google.com/sdk/gcloud/
[9]: https://pypi.python.org/pypi/oauth2client
[10]: https://github.com/grpc/grpc/issues/2611
[11]: https://github.com/grpc/grpc/tree/master/src/python
[12]: https://pypi.python.org/pypi/grpcio
[14]: https://console.developers.google.com/
