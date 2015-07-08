GENERATED_DIR=$(shell pwd)/generated_python
FINAL_DIR="gcloud_bigtable/_generated"

help:
	@echo 'Makefile for a gcloud-python-bigtable                  '
	@echo '                                                       '
	@echo '   make generate         Generates the protobuf modules'
	@echo '   make check_generate   Checks that generate succeeded'
	@echo '   make clean            Clean generated files         '

generate:
	[ -d cloud-bigtable-client ] || git clone https://github.com/GoogleCloudPlatform/cloud-bigtable-client
	cd cloud-bigtable-client && git pull origin master
	mkdir -p $(GENERATED_DIR)
	# Data API
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/bigtable/v1/*.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/bigtable/v1/* $(FINAL_DIR)
	# Cluster API
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/bigtable/admin/cluster/v1/*.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/bigtable/admin/cluster/v1/* $(FINAL_DIR)
	# Table API
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/bigtable/admin/table/v1/*.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/bigtable/admin/table/v1/* $(FINAL_DIR)
	# Auxiliary protos
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/api/*.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/api/* $(FINAL_DIR)
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/protobuf/any.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/protobuf/any_pb2.py $(FINAL_DIR)
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/protobuf/duration.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/protobuf/duration_pb2.py $(FINAL_DIR)
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/protobuf/empty.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/protobuf/empty_pb2.py $(FINAL_DIR)
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/protobuf/timestamp.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/protobuf/timestamp_pb2.py $(FINAL_DIR)
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/longrunning/operations.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/longrunning/operations_pb2.py $(FINAL_DIR)
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/rpc/status.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/rpc/status_pb2.py $(FINAL_DIR)
	python scripts/rewrite_imports.py

check_generate:
	python scripts/check_generate.py

clean:
	rm -fr cloud-bigtable-client $(GENERATED_DIR)

.PHONY: generate check_generate clean
