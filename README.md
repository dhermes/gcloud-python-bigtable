# Google Cloud Bigtable Python Library (Alpha)

### An extension to [`gcloud-python`][1]

This library supports RPC requests to the Google Cloud Bigtable API over
HTTP/2. In order to support this, we'll rely on [`grpc`][2]. Unfortunately,
the install story of gRPC is still developing. We are working with the
gRPC team to rapidly make the install story more user-friendly.

The Cloud Bigtable API is supported via JSON over HTTP/1.1 (see the
[`gcloud` CLI][4] use of the API). However, features of HTTP/2 such
as streaming are [used][5] by the Bigtable API and are not possible
to support via HTTP/1.1.

## Development

See [`CONTRIBUTING.md`][3] for instructions on development.

[1]: https://github.com/GoogleCloudPlatform/gcloud-python
[2]: https://www.grpc.io/
[3]: https://github.com/dhermes/gcloud-python-bigtable/blob/master/CONTRIBUTING.md
[4]: https://cloud.google.com/sdk/gcloud/reference/alpha/bigtable/clusters/list
[5]: https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/e6fc386d9adc821e1cf5c175c5bf5830b641eb3f/bigtable-protos/src/main/proto/google/bigtable/v1/bigtable_service.proto#L36-L46
