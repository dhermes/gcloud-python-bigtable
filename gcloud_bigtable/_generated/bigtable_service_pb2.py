# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/bigtable/v1/bigtable_service.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from gcloud_bigtable._generated import annotations_pb2 as google_dot_api_dot_annotations__pb2
from gcloud_bigtable._generated import bigtable_data_pb2 as google_dot_bigtable_dot_v1_dot_bigtable__data__pb2
from gcloud_bigtable._generated import bigtable_service_messages_pb2 as google_dot_bigtable_dot_v1_dot_bigtable__service__messages__pb2
from gcloud_bigtable._generated import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='google/bigtable/v1/bigtable_service.proto',
  package='google.bigtable.v1',
  syntax='proto3',
  serialized_pb=_b('\n)google/bigtable/v1/bigtable_service.proto\x12\x12google.bigtable.v1\x1a\x1cgoogle/api/annotations.proto\x1a&google/bigtable/v1/bigtable_data.proto\x1a\x32google/bigtable/v1/bigtable_service_messages.proto\x1a\x1bgoogle/protobuf/empty.proto2\xb0\x07\n\x0f\x42igtableService\x12\xa5\x01\n\x08ReadRows\x12#.google.bigtable.v1.ReadRowsRequest\x1a$.google.bigtable.v1.ReadRowsResponse\"L\x82\xd3\xe4\x93\x02\x46\"A/v1/{table_name=projects/*/zones/*/clusters/*/tables/*}/rows:read:\x01*0\x01\x12\xb7\x01\n\rSampleRowKeys\x12(.google.bigtable.v1.SampleRowKeysRequest\x1a).google.bigtable.v1.SampleRowKeysResponse\"O\x82\xd3\xe4\x93\x02I\x12G/v1/{table_name=projects/*/zones/*/clusters/*/tables/*}/rows:sampleKeys0\x01\x12\xa3\x01\n\tMutateRow\x12$.google.bigtable.v1.MutateRowRequest\x1a\x16.google.protobuf.Empty\"X\x82\xd3\xe4\x93\x02R\"M/v1/{table_name=projects/*/zones/*/clusters/*/tables/*}/rows/{row_key}:mutate:\x01*\x12\xd2\x01\n\x11\x43heckAndMutateRow\x12,.google.bigtable.v1.CheckAndMutateRowRequest\x1a-.google.bigtable.v1.CheckAndMutateRowResponse\"`\x82\xd3\xe4\x93\x02Z\"U/v1/{table_name=projects/*/zones/*/clusters/*/tables/*}/rows/{row_key}:checkAndMutate:\x01*\x12\xbf\x01\n\x12ReadModifyWriteRow\x12-.google.bigtable.v1.ReadModifyWriteRowRequest\x1a\x17.google.bigtable.v1.Row\"a\x82\xd3\xe4\x93\x02[\"V/v1/{table_name=projects/*/zones/*/clusters/*/tables/*}/rows/{row_key}:readModifyWrite:\x01*B4\n\x16\x63om.google.bigtable.v1B\x15\x42igtableServicesProtoP\x01\x88\x01\x01\x62\x06proto3')
  ,
  dependencies=[google_dot_api_dot_annotations__pb2.DESCRIPTOR,google_dot_bigtable_dot_v1_dot_bigtable__data__pb2.DESCRIPTOR,google_dot_bigtable_dot_v1_dot_bigtable__service__messages__pb2.DESCRIPTOR,google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)





DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\n\026com.google.bigtable.v1B\025BigtableServicesProtoP\001\210\001\001'))
import abc
from grpc.early_adopter import implementations
from grpc.framework.alpha import utilities
class EarlyAdopterBigtableServiceServicer(object):
  """<fill me in later!>"""
  __metaclass__ = abc.ABCMeta
  @abc.abstractmethod
  def ReadRows(self, request, context):
    raise NotImplementedError()
  @abc.abstractmethod
  def SampleRowKeys(self, request, context):
    raise NotImplementedError()
  @abc.abstractmethod
  def MutateRow(self, request, context):
    raise NotImplementedError()
  @abc.abstractmethod
  def CheckAndMutateRow(self, request, context):
    raise NotImplementedError()
  @abc.abstractmethod
  def ReadModifyWriteRow(self, request, context):
    raise NotImplementedError()
class EarlyAdopterBigtableServiceServer(object):
  """<fill me in later!>"""
  __metaclass__ = abc.ABCMeta
  @abc.abstractmethod
  def start(self):
    raise NotImplementedError()
  @abc.abstractmethod
  def stop(self):
    raise NotImplementedError()
class EarlyAdopterBigtableServiceStub(object):
  """<fill me in later!>"""
  __metaclass__ = abc.ABCMeta
  @abc.abstractmethod
  def ReadRows(self, request):
    raise NotImplementedError()
  ReadRows.async = None
  @abc.abstractmethod
  def SampleRowKeys(self, request):
    raise NotImplementedError()
  SampleRowKeys.async = None
  @abc.abstractmethod
  def MutateRow(self, request):
    raise NotImplementedError()
  MutateRow.async = None
  @abc.abstractmethod
  def CheckAndMutateRow(self, request):
    raise NotImplementedError()
  CheckAndMutateRow.async = None
  @abc.abstractmethod
  def ReadModifyWriteRow(self, request):
    raise NotImplementedError()
  ReadModifyWriteRow.async = None
def early_adopter_create_BigtableService_server(servicer, port, private_key=None, certificate_chain=None):
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.empty_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_data_pb2
  method_service_descriptions = {
    "CheckAndMutateRow": utilities.unary_unary_service_description(
      servicer.CheckAndMutateRow,
      google.bigtable.v1.bigtable_service_messages_pb2.CheckAndMutateRowRequest.FromString,
      google.bigtable.v1.bigtable_service_messages_pb2.CheckAndMutateRowResponse.SerializeToString,
    ),
    "MutateRow": utilities.unary_unary_service_description(
      servicer.MutateRow,
      google.bigtable.v1.bigtable_service_messages_pb2.MutateRowRequest.FromString,
      google.protobuf.empty_pb2.Empty.SerializeToString,
    ),
    "ReadModifyWriteRow": utilities.unary_unary_service_description(
      servicer.ReadModifyWriteRow,
      google.bigtable.v1.bigtable_service_messages_pb2.ReadModifyWriteRowRequest.FromString,
      google.bigtable.v1.bigtable_data_pb2.Row.SerializeToString,
    ),
    "ReadRows": utilities.unary_stream_service_description(
      servicer.ReadRows,
      google.bigtable.v1.bigtable_service_messages_pb2.ReadRowsRequest.FromString,
      google.bigtable.v1.bigtable_service_messages_pb2.ReadRowsResponse.SerializeToString,
    ),
    "SampleRowKeys": utilities.unary_stream_service_description(
      servicer.SampleRowKeys,
      google.bigtable.v1.bigtable_service_messages_pb2.SampleRowKeysRequest.FromString,
      google.bigtable.v1.bigtable_service_messages_pb2.SampleRowKeysResponse.SerializeToString,
    ),
  }
  return implementations.server("google.bigtable.v1.BigtableService", method_service_descriptions, port, private_key=private_key, certificate_chain=certificate_chain)
def early_adopter_create_BigtableService_stub(host, port, metadata_transformer=None, secure=False, root_certificates=None, private_key=None, certificate_chain=None, server_host_override=None):
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.empty_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_service_messages_pb2
  import gcloud_bigtable._generated.bigtable_data_pb2
  method_invocation_descriptions = {
    "CheckAndMutateRow": utilities.unary_unary_invocation_description(
      google.bigtable.v1.bigtable_service_messages_pb2.CheckAndMutateRowRequest.SerializeToString,
      google.bigtable.v1.bigtable_service_messages_pb2.CheckAndMutateRowResponse.FromString,
    ),
    "MutateRow": utilities.unary_unary_invocation_description(
      google.bigtable.v1.bigtable_service_messages_pb2.MutateRowRequest.SerializeToString,
      google.protobuf.empty_pb2.Empty.FromString,
    ),
    "ReadModifyWriteRow": utilities.unary_unary_invocation_description(
      google.bigtable.v1.bigtable_service_messages_pb2.ReadModifyWriteRowRequest.SerializeToString,
      google.bigtable.v1.bigtable_data_pb2.Row.FromString,
    ),
    "ReadRows": utilities.unary_stream_invocation_description(
      google.bigtable.v1.bigtable_service_messages_pb2.ReadRowsRequest.SerializeToString,
      google.bigtable.v1.bigtable_service_messages_pb2.ReadRowsResponse.FromString,
    ),
    "SampleRowKeys": utilities.unary_stream_invocation_description(
      google.bigtable.v1.bigtable_service_messages_pb2.SampleRowKeysRequest.SerializeToString,
      google.bigtable.v1.bigtable_service_messages_pb2.SampleRowKeysResponse.FromString,
    ),
  }
  return implementations.stub("google.bigtable.v1.BigtableService", method_invocation_descriptions, host, port, metadata_transformer=metadata_transformer, secure=secure, root_certificates=root_certificates, private_key=private_key, certificate_chain=certificate_chain, server_host_override=server_host_override)
# @@protoc_insertion_point(module_scope)
