GENERATED_DIR=$(shell pwd)/generated_python
FINAL_DIR="gcloud_bigtable/_generated"
GRPC_PLUGIN="$(HOME)/.linuxbrew/bin/grpc_python_plugin"

help:
	@echo 'Makefile for a gcloud-python-bigtable                           '
	@echo '                                                                '
	@echo '   make generate                 Generates the protobuf modules '
	@echo '   make check_generate           Checks that generate succeeded '
	@echo '   make setup_hello_world        Sets up Hello world example    '
	@echo '   make run_hello_world_server   Runs Hello world example server'
	@echo '   make consume_hello_world      Make request to example server '
	@echo '   make clean                    Clean generated files          '

generate:
	[ -d cloud-bigtable-client ] || git clone https://github.com/GoogleCloudPlatform/cloud-bigtable-client
	cd cloud-bigtable-client && git pull origin master
	mkdir -p $(GENERATED_DIR)
	# Data API
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) google/bigtable/v1/*.proto
	# Cluster API
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/bigtable/admin/cluster/v1/*.proto
	# Table API
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/bigtable/admin/table/v1/*.proto
	# Auxiliary protos
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/api/*.proto
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/protobuf/any.proto
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/protobuf/duration.proto
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/protobuf/empty.proto
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/protobuf/timestamp.proto
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/longrunning/operations.proto
	cd cloud-bigtable-client/bigtable-protos/src/main/proto && \
	    protoc --python_out=$(GENERATED_DIR) --grpc_out=$(GENERATED_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) \
	    google/rpc/status.proto
	rm -fr $(FINAL_DIR)  # Reset the directory
	mkdir -p $(FINAL_DIR)
	mv $(GENERATED_DIR)/google $(FINAL_DIR)
	python scripts/add_init_files.py
	python scripts/add_absolute_imports.py

check_generate:
	LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(HOME)/.linuxbrew/lib python scripts/check_generate.py

clean:
	rm -fr cloud-bigtable-client $(GENERATED_DIR)

setup_hello_world:
	mkdir -p hello_world
	[ -f hello_world/helloworld.proto ] || curl https://raw.githubusercontent.com/grpc/grpc-common/master/protos/helloworld.proto -o hello_world/helloworld.proto
	[ -f hello_world/greeter_server.py ] || curl https://raw.githubusercontent.com/grpc/grpc-common/master/python/helloworld/greeter_server.py -o hello_world/greeter_server.py
	[ -f hello_world/greeter_client.py ] || curl https://raw.githubusercontent.com/grpc/grpc-common/master/python/helloworld/greeter_client.py -o hello_world/greeter_client.py
	cd hello_world && protoc --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) helloworld.proto
	cd hello_world && LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(HOME)/.linuxbrew/lib python -c 'import helloworld_pb2'

run_hello_world_server: setup_hello_world
	cd hello_world && LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(HOME)/.linuxbrew/lib python greeter_server.py

consume_hello_world: setup_hello_world
	cd hello_world && LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(HOME)/.linuxbrew/lib python greeter_client.py

.PHONY: generate check_generate clean setup_hello_world run_hello_world_server consume_hello_world
