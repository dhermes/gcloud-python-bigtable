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

[1]: https://github.com/GoogleCloudPlatform/gcloud-python
[2]: https://www.grpc.io/
[3]: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/tree/master/bigtable-protos/src/main/proto/google/bigtable/v1
