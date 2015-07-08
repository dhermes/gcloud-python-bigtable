GENERATED_DIR=$(shell pwd)/generated_python

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
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/bigtable/v1/*.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/bigtable/v1/* gcloud_bigtable
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc google/api/*.proto --python_out=$(GENERATED_DIR)
	mv $(GENERATED_DIR)/google/api/* gcloud_bigtable
	python scripts/rewrite_imports.py

check_generate:
	python -c "import gcloud_bigtable"
	python -c "from gcloud_bigtable import http_pb2"
	python -c "from gcloud_bigtable import bigtable_data_pb2"
	python -c "from gcloud_bigtable import bigtable_service_messages_pb2"
	python -c "from gcloud_bigtable import bigtable_service_pb2"
	python -c "from gcloud_bigtable import annotations_pb2"

clean:
	rm -fr cloud-bigtable-client $(GENERATED_DIR)

.PHONY: generate check_generate clean
