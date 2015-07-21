# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Connection to Google Cloud Operations API."""

from gcloud_bigtable._generated import operations_pb2
from gcloud_bigtable.connection import Connection
from gcloud_bigtable.connection import MetadataTransformer
from gcloud_bigtable.connection import get_certs


OPERATIONS_STUB_FACTORY = operations_pb2.early_adopter_create_Operations_stub
PORT = 443


def make_operations_stub(host, credentials):
    """Makes a stub for the Operations API.

    :type host: string
    :param host: The host for the operations service. This is not specified
                 as a module level constant, since the host will correspond
                 to the service which generated the long-running operation.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials`
    :param credentials: The OAuth2 Credentials to use for access tokens
                        to authorize requests.

    :rtype: :class:`grpc.early_adopter.implementations._Stub`
    :returns: The stub object used to make gRPC requests to the
              Operations API.
    """
    custom_metadata_transformer = MetadataTransformer(credentials)
    return OPERATIONS_STUB_FACTORY(
        host, PORT,
        metadata_transformer=custom_metadata_transformer,
        secure=True,
        root_certificates=get_certs())


class OperationsConnection(Connection):
    """Connection to Google Cloud Operations API."""

    def get_operation(self):
        """Gets a long-running operation."""
        # GetOperation: GetOperationRequest --> Operation
        raise NotImplementedError

    def list_operations(self):
        """Lists all long-running operations."""
        # ListOperations: ListOperationsRequest --> ListOperationsResponse
        raise NotImplementedError

    def cancel_operation(self):
        """Cancels a long-running operation."""
        # CancelOperation: CancelOperationRequest --> google.protobuf.Empty
        raise NotImplementedError

    def delete_operation(self):
        """Deletes a long-running operation."""
        # DeleteOperation: DeleteOperationRequest --> google.protobuf.Empty
        raise NotImplementedError
