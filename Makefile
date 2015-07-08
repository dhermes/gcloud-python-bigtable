GENERATED_DIR=$(shell pwd)/generated_python

help:
	@echo 'Makefile for a gcloud-python-bigtable            '
	@echo '                                                 '
	@echo '   make generate   Generates the protobuf modules'
	@echo '   make clean      Clean generated files         '

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

clean:
	rm -fr cloud-bigtable-client $(GENERATED_DIR)

.PHONY: generate clean
