# Google Cloud BigTable Python Library (Alpha)

### An extension to [`gcloud-python`][1]

Currently, due to the install story of [`grpc`][2], this library will only
support the Cloud BigTable API over HTTP/1.1. We are working with the
gRPC team to rapidly make the install story more user-friendly.

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

```bash
$ git clone https://github.com/GoogleCloudPlatform/cloud-bigtable-client
$ PYTHON_DIR="$(pwd)/generated_python"
$ mkdir -p ${PYTHON_DIR}
$ cd cloud-bigtable-client/bigtable-protos/src/main/proto
$ protoc google/bigtable/v1/*.proto --python_out=${PYTHON_DIR}
$ cd ${PYTHON_DIR}/..
$ rm -fr cloud-bigtable-client  # Clean up the cloned directory
$ # Move generated files into repo
$ mv ${PYTHON_DIR}/google/bigtable/v1/* gcloud_bigtable
$ rm -fr ${PYTHON_DIR}  # Clean up the temporary directory
```

[1]: https://github.com/GoogleCloudPlatform/gcloud-python
[2]: https://www.grpc.io/
[3]: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/tree/master/bigtable-protos/src/main/proto/google/bigtable/v1
[4]: https://github.com/google/protobuf
