# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: google/protobuf/timestamp.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='google/protobuf/timestamp.proto',
  package='google.protobuf',
  syntax='proto3',
  serialized_pb=b'\n\x1fgoogle/protobuf/timestamp.proto\x12\x0fgoogle.protobuf\"+\n\tTimestamp\x12\x0f\n\x07seconds\x18\x01 \x01(\x03\x12\r\n\x05nanos\x18\x02 \x01(\x05\x42*\n\x13\x63om.google.protobufB\x0eTimestampProtoP\x01\xa0\x01\x01\x62\x06proto3'
)
_sym_db.RegisterFileDescriptor(DESCRIPTOR)




_TIMESTAMP = _descriptor.Descriptor(
  name='Timestamp',
  full_name='google.protobuf.Timestamp',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='seconds', full_name='google.protobuf.Timestamp.seconds', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='nanos', full_name='google.protobuf.Timestamp.nanos', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
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
  serialized_start=52,
  serialized_end=95,
)

DESCRIPTOR.message_types_by_name['Timestamp'] = _TIMESTAMP

Timestamp = _reflection.GeneratedProtocolMessageType('Timestamp', (_message.Message,), dict(
  DESCRIPTOR = _TIMESTAMP,
  __module__ = 'google.protobuf.timestamp_pb2'
  # @@protoc_insertion_point(class_scope:google.protobuf.Timestamp)
  ))
_sym_db.RegisterMessage(Timestamp)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), b'\n\023com.google.protobufB\016TimestampProtoP\001\240\001\001')
# @@protoc_insertion_point(module_scope)
