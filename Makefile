#
# Copyright 2015 gRPC authors.
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
#

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += -Igrpc/include -Igrpc/third_party/protobuf/src -Itemp
CXXFLAGS += -std=c++11 -O2 -Wall -Werror
ifeq ($(SYSTEM),Darwin)
LDFLAGS += -O2 -Lgrpc/libs/opt -Lgrpc/third_party/protobuf/src/.libs -Lgrpc/third_party/zlib \
           -lgrpc++_reflection\
           -ldl -lgrpc -lgrpc++ -lgpr -lgrpc_unsecure -lprotobuf -lpthread -lz -static
else
LDFLAGS += -O2 -Lgrpc/libs/opt -Lgrpc/third_party/protobuf/src/.libs -Lgrpc/third_party/zlib \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl -lgrpc -lgrpc++ -lgpr -lgrpc_unsecure -lprotobuf -lpthread -lz -static
endif
PROTOC = grpc/third_party/protobuf/src/protoc
GRPC_CPP_PLUGIN_PATH = grpc/bins/opt/grpc_cpp_plugin

BIN_PATH = bin
SRC_PATH = src
TEMP_PATH = temp

# Actual targets

all: $(BIN_PATH) $(TEMP_PATH) $(BIN_PATH)/gfs_client $(BIN_PATH)/gfs_server

$(BIN_PATH)/gfs_client: $(TEMP_PATH)/gfs.pb.o $(TEMP_PATH)/gfs.grpc.pb.o $(TEMP_PATH)/gfs_client.o
	$(CXX) $(filter %.o,$^) $(LDFLAGS) -o $@

$(BIN_PATH)/gfs_server: $(TEMP_PATH)/gfs.pb.o $(TEMP_PATH)/gfs.grpc.pb.o $(TEMP_PATH)/gfs_server.o
	$(CXX) $(filter %.o,$^) $(LDFLAGS) -o $@

# End actual targets

clean:
	rm -rf $(BIN_PATH) $(TEMP_PATH)

$(BIN_PATH):
	mkdir -p $(BIN_PATH)

$(TEMP_PATH):
	mkdir -p $(TEMP_PATH)

.PRECIOUS: $(TEMP_PATH)/%.grpc.pb.cc
$(TEMP_PATH)/%.grpc.pb.cc: $(SRC_PATH)/%.proto
	$(PROTOC) -I $(SRC_PATH) --grpc_out=$(TEMP_PATH) --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

.PRECIOUS: $(TEMP_PATH)/%.pb.cc
$(TEMP_PATH)/%.pb.cc: $(SRC_PATH)/%.proto
	$(PROTOC) -I $(SRC_PATH) --cpp_out=$(TEMP_PATH) $<

$(TEMP_PATH)/%.o: $(SRC_PATH)/%.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c -o $@ $<

$(TEMP_PATH)/%.pb.o: $(TEMP_PATH)/%.pb.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c -o $@ $<
