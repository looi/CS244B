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
#include <fstream>
#include <memory>
#include <string>
#include <inttypes.h>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::PingRequest;
using gfs::PingReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::GFS;
using gfs::ErrorCode;

// Logic and data behind the server's behavior.
class GFSServiceImpl final : public GFS::Service {
public:
  GFSServiceImpl(std::string path) {
    this->full_path = path;
  }

  Status ClientServerPing(ServerContext* context, const PingRequest* request,
                          PingReply* reply) override {
    std::string prefix("Hello ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }

  Status ReadChunk(ServerContext* context, const ReadChunkRequest* request,
                   ReadChunkReply* reply) override {
    std::string chunk_data;
    std::ifstream infile;
    std::string filename = this->full_path + "/" +
        std::to_string(request->chunkhandle());

    infile.open(filename.c_str(), std::ios::in);
    if (!infile) {
      std::cout << "can't open file for reading: " << filename << std::endl;
      reply->set_error_code(ErrorCode::FAILED);
    } else {
      infile >> chunk_data;
      infile.close();
      reply->set_data(chunk_data);
      reply->set_error_code(ErrorCode::SUCCESS);
    }

    return Status::OK;
  }

  Status WriteChunk(ServerContext* context, const WriteChunkRequest* request,
                    WriteChunkReply* reply) override {
    std::cout << "Got server WriteChunk for chunkhandle = " << \
              request->chunkhandle() << " and data = " << request->data() << \
              std::endl;

    std::ofstream outfile;
    std::string filename = this->full_path + "/" +
        std::to_string(request->chunkhandle());
    outfile.open(filename.c_str(), std::ios::out);
    if (!outfile) {
      std::cout << "can't open file for writing: " << filename << std::endl;
      reply->set_error_code(ErrorCode::FAILED);
    } else {
      outfile << request->data();
      outfile.close();
      reply->set_error_code(ErrorCode::SUCCESS);
    }
    return Status::OK;
  }

 private:
  std::string full_path;
};

void RunServer(std::string path) {
  std::string server_address("127.0.0.1:50051");
  GFSServiceImpl service(path);

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "Usage: gfs_server path_to_local_file_directory" << std::endl;
    return 1;
  }
  RunServer(argv[1]);

  return 0;
}
