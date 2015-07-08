# Google Cloud BigTable Python Library (Alpha)

### An extension to [`gcloud-python`][1]

The Google Cloud BigTable API only supports requests over HTTP/2.
In order to suppor this, we'll rely on [`grpc`][2]. Unfortunately,
the install story of gRPC is still developing. We are working with the
gRPC team to rapidly make the install story more user-friendly.

In it's current form, this library attempted to support requests
over HTTP/1.1. This is not allowed by the backend, and results in
a 400 Bad Request:

```json
{
  "error": {
    "code": 400,
    "message": "Proto over HTTP is not allowed for service 'bigtableclusteradmin.googleapis.com'.",
    "status": "FAILED_PRECONDITION"
  }
}
```

## Building `_pb2.py` files

Calling the Google Cloud BigTable API requires

```
gcloud_bigtable/bigtable_data_pb2.py
gcloud_bigtable/bigtable_service_messages_pb2.py
gcloud_bigtable/bigtable_service_pb2.py
```

These are built from the [`.proto` files][3] provided with the
Cloud BigTable client. Compiling them to Python files requires
the latest version of the `protoc` compiler to support the
`proto3` syntax.

In order to install `protoc` version `>= 3.0.0`, follow the
[instructions][4] on the project. They are roughly

```bash
$ sudo apt-get install autoconf libtool
$ git clone https://github.com/google/protobuf
$ cd protobuf
$ # Optionally checkout a tagged commit: git checkout v3.0.0-alpha-3.1
$ ./autogen.sh  # Generate the configure script
$ # Build and install C++ Protocol Buffer runtime and
$ # the Protocol Buffer compiler (protoc)
$ ./configure [--prefix=/usr/local]  # or [--prefix=/usr]
$ make
$ make check
$ # Make sure that `make check` passes
$ [sudo] make install
$ # May need to update dynamic linker config via:
$ # [sudo] ldconfig
```

After installing `protoc` version `>= 3.0.0`, you can generate the
protobuf generated modules via

```bash
$ make generate
```

Also be sure to have the latest version of the Python [`protobuf`][5]
library:

```bash
$ [sudo] pip install "protobuf>=3.0.0a3"
```

[1]: https://github.com/GoogleCloudPlatform/gcloud-python
[2]: https://www.grpc.io/
[3]: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/tree/master/bigtable-protos/src/main/proto/google/bigtable/v1
[4]: https://github.com/google/protobuf
[5]: https://pypi.python.org/pypi/protobuf
