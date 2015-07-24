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
from gcloud_bigtable.connection import TIMEOUT_SECONDS
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


def _prepare_list_request(filter=None, page_size=None, page_token=None):
    """Make a list request object.

    :type filter: string
    :param filter: (Optional) The filter for the list request.

    :type page_size: integer
    :param page_size: (Optional) The size of a single page of results.

    :type page_token: string
    :param page_token: (Optional) The token to begin paging through
                       a results set.

    :rtype: :class:`operations_pb2.ListOperationsRequest`
    :returns: The list operations request object that was created.
    """
    list_kwargs = {'name': 'operations'}
    if filter is not None:
        list_kwargs['filter'] = filter
    if page_size is not None:
        list_kwargs['page_size'] = page_size
    if page_token is not None:
        list_kwargs['page_token'] = page_token
    return operations_pb2.ListOperationsRequest(**list_kwargs)


class OperationsConnection(Connection):
    """Connection to Google Cloud Operations API.

    :type host: string
    :param host: The host for the operations service.

    :type scope: string or iterable of strings
    :param scope: The effective service auth scopes for the connection.

    :type credentials: :class:`oauth2client.client.OAuth2Credentials` or
                       :class:`NoneType`
    :param credentials: The OAuth2 Credentials to use for this connection.
    """

    def __init__(self, host, scope=None, credentials=None):
        self._host = host
        setattr(self, 'SCOPE', scope)  # To override the global.
        super(OperationsConnection, self).__init__(credentials)

    def get_operation(self, operation_name, timeout_seconds=TIMEOUT_SECONDS):
        """Gets a long-running operation.

        :type operation_name: string
        :param operation_name: The operation being retrieved. Must be of the
                               form "operations/**".

        :type timeout_seconds: integer
        :param timeout_seconds: (Optional) Number of seconds for request
                                time-out. If not passed, defaults to
                                ``TIMEOUT_SECONDS``.

        :rtype: :class:`operations_pb2.Operations`
        :returns: The operation retrieved.
        """
        request_pb = operations_pb2.GetOperationRequest(name=operation_name)
        result_pb = None
        with make_operations_stub(self._host, self._credentials) as stub:
            response = stub.GetOperation.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def list_operations(self, filter=None, page_size=None,
                        page_token=None, timeout_seconds=TIMEOUT_SECONDS):
        """Lists all long-running operations.

        .. note::
          If the server doesn't support this method, it returns
          ``google.rpc.Code.UNIMPLEMENTED``.

        :type filter: string
        :param filter: (Optional) The filter for the list request.

        :type page_size: integer
        :param page_size: (Optional) The size of a single page of results.

        :type page_token: string
        :param page_token: (Optional) The token to begin paging through
                           a results set.

        :type timeout_seconds: integer
        :param timeout_seconds: (Optional) Number of seconds for request
                                time-out. If not passed, defaults to
                                ``TIMEOUT_SECONDS``.

        :rtype: :class:`operations_pb2.ListOperationsResponse`
        :returns: The list operations response object.
        """
        request_pb = _prepare_list_request(filter=filter, page_size=page_size,
                                           page_token=page_token)
        result_pb = None
        with make_operations_stub(self._host, self._credentials) as stub:
            response = stub.ListOperations.async(request_pb, timeout_seconds)
            result_pb = response.result()

        return result_pb

    def cancel_operation(self):
        """Cancels a long-running operation."""
        # CancelOperation: CancelOperationRequest --> google.protobuf.Empty
        raise NotImplementedError

    def delete_operation(self):
        """Deletes a long-running operation."""
        # DeleteOperation: DeleteOperationRequest --> google.protobuf.Empty
        raise NotImplementedError
