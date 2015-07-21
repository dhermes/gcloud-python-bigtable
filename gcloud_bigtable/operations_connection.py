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

from gcloud_bigtable.connection import Connection


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
