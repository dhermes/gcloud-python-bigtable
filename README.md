# Google Cloud BigTable Python Library (Alpha)

### An extension to [`gcloud-python`][1]

The Google Cloud BigTable API only supports requests over HTTP/2.
In order to support this, we'll rely on [`grpc`][2]. Unfortunately,
the install story of gRPC is still developing. We are working with the
gRPC team to rapidly make the install story more user-friendly.

In its current form, this library attempted to support HTTP-RPC requests
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

## Development

See [`CONTRIBUTING.md`][3] for instructions on development.

[1]: https://github.com/GoogleCloudPlatform/gcloud-python
[2]: https://www.grpc.io/
[3]: https://github.com/dhermes/gcloud-python-bigtable/blob/master/CONTRIBUTING.md
