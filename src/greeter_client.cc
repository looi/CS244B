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
#include "helloworld.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::ReadChunkRequest;
using helloworld::ReadChunkReply;
using helloworld::WriteChunkRequest;
using helloworld::WriteChunkReply;
using helloworld::Greeter;
using helloworld::ErrorCode;

class GreeterClient {
 public:
  GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string SayHello(const std::string& user) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(user);

    // Container for the data we expect from the server.
    HelloReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->SayHello(&context, request, &reply);

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
      std::cout << "Greeter Write Chunk returned: " << reply.error_code() << \
                std::endl;
      return "RPC succeeded";
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GreeterClient greeter(grpc::CreateChannel(
      "127.0.0.1:50051", grpc::InsecureChannelCredentials()));
  std::string user("world");
  std::string reply = greeter.SayHello(user);
  std::cout << "Greeter received: " << reply << std::endl;

  std::string data = greeter.ReadChunk(10);
  std::cout << "Greeter received chunk data: " << data << std::endl;

  std::string rpc_result = greeter.WriteChunk(10, data);
  return 0;
}
