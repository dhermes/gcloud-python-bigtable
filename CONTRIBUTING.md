## Building `_pb2.py` files

The `gcloud_bigtable/_generated` directory depends on files
generated from [`bigtable-protos`][1]. These generated
files must be re-generated from time to time.

To generate them, execute

```bash
make generate
```

Compiling them to Python files requires
-   the latest version of the `protoc` compiler to support the `proto3` syntax
-   the `grpc_python_plugin` (which requires installing `grpc`)
-   the latest version of the Python [`protobuf`][3] library:

    ```bash
    $ [sudo] pip install "protobuf>=3.0.0a3"
    ```

## Installing `protoc>=3.0.0`

In order to install `protoc` version `>= 3.0.0`, follow the
[instructions][2] on the project. For Debian-based Linux, they are roughly

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

## Testing

Unfortunately, `tox` will fail by default due to the absence of
`grpcio` from `setup.py`. In order to successfully set up a `tox`
test environment (e.g `ENV=py27`), run

```bash
tox -e ${ENV}
```

and watch the install fail (missing dependencies), then with the partially
set up environment, execute:

```bash
BREW_PREFIX=$(brew --prefix)
VERSION=0.10.0a0
CFLAGS=-I${BREW_PREFIX}/include LDFLAGS=-L${BREW_PREFIX}/lib \
  .tox/${ENV}/bin/pip install grpcio==${VERSION}
unset BREW_PREFIX
```

## False Starts

This library originally attempted to support HTTP-RPC requests
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

[1]: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/tree/master/bigtable-protos/src/main/proto/google/
[2]: https://github.com/google/protobuf
[3]: https://pypi.python.org/pypi/protobuf
