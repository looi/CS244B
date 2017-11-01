/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <inttypes.h>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using gfs::PingRequest;
using gfs::PingReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::GFS;
using gfs::ErrorCode;

class GFSClient {
 public:
  GFSClient(std::shared_ptr<Channel> channel)
      : stub_(GFS::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string ClientServerPing(const std::string& user) {
    // Data we are sending to the server.
    PingRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    PingReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->ClientServerPing(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  // Client ReadChunk implementation
  std::string ReadChunk(const int chunk_id) {
    // Data we are sending to the server.
    ReadChunkRequest request;
    request.set_chunk_id(chunk_id);

    // Container for the data we expect from the server.
    ReadChunkReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->ReadChunk(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      return reply.data();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  // Client WriteChunk implementation
  std::string WriteChunk(const int chunk_id, const std::string data) {
    // Data we are sending to the server.
    WriteChunkRequest request;
    request.set_chunk_id(chunk_id);
    request.set_data(data);

    // Container for the data we expect from the server.
    WriteChunkReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->WriteChunk(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      std::cout << "Write Chunk returned: " << reply.error_code() << \
                std::endl;
      return "RPC succeeded";
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<GFS::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(grpc::CreateChannel(
      "127.0.0.1:50051", grpc::InsecureChannelCredentials()));
  std::string user("world");
  for (int i = 0; i < 1; i++) {
    std::string reply = gfs_client.ClientServerPing(user);
    std::cout << "Client received: " << reply << std::endl;

    std::string data = gfs_client.ReadChunk(10);
    std::cout << "Client received chunk data: " << data << std::endl;

    std::string rpc_result = gfs_client.WriteChunk(10, data);
  }
  return 0;
}
