from __future__ import absolute_import
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/protobuf/empty.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='google/protobuf/empty.proto',
  package='google.protobuf',
  syntax='proto3',
  serialized_pb=_b('\n\x1bgoogle/protobuf/empty.proto\x12\x0fgoogle.protobuf\"\x07\n\x05\x45mptyB#\n\x13\x63om.google.protobufB\nEmptyProtoP\x01\x62\x06proto3')
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_EMPTY = _descriptor.Descriptor(
  name='Empty',
  full_name='google.protobuf.Empty',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=48,
  serialized_end=55,
)

DESCRIPTOR.message_types_by_name['Empty'] = _EMPTY

Empty = _reflection.GeneratedProtocolMessageType('Empty', (_message.Message,), dict(
  DESCRIPTOR = _EMPTY,
  __module__ = 'google.protobuf.empty_pb2'
  # @@protoc_insertion_point(class_scope:google.protobuf.Empty)
  ))
_sym_db.RegisterMessage(Empty)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\n\023com.google.protobufB\nEmptyProtoP\001'))
import abc
from grpc.early_adopter import implementations
from grpc.framework.alpha import utilities
# @@protoc_insertion_point(module_scope)
