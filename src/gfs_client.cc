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

#include "gfs_client.h"

#include <iostream>
#include <memory>
#include <string>
#include <inttypes.h>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using gfs::FindLeaseHolderRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindMatchingFilesRequest;
using gfs::FindMatchingFilesReply;
using gfs::PingRequest;
using gfs::PingReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::GFS;
using gfs::GFSMaster;

// Client's main function
int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel("127.0.0.1:50051", grpc::InsecureChannelCredentials()),
      grpc::CreateChannel("127.0.0.1:50052", grpc::InsecureChannelCredentials()));
  std::string user("world");
  for (int i = 0; i < 10; i++) {
    // int length;
    std::string data("new#data" + std::to_string(i));
    std::string reply = gfs_client.ClientServerPing(user);
    std::cout << "Client received: " << reply << std::endl;

    std::string rpc_result = gfs_client.WriteChunk(i, data, 0);
    data = gfs_client.ReadChunk(i, 0, data.length());
    std::cout << "Client received chunk data: " << data << std::endl;
  }
  gfs_client.FindLeaseHolder("a/aa.txt", 0);
  gfs_client.FindLeaseHolder("a/ab.txt", 0);
  gfs_client.FindLeaseHolder("a/aa.txt", 0);
  gfs_client.FindLeaseHolder("a/aa.txt", 1);
  gfs_client.FindLeaseHolder("a/b.txt", 0);
  gfs_client.FindMatchingFiles("a/a");
  return 0;
}

std::string GFSClient::ClientServerPing(const std::string& user) {
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

std::string GFSClient::ReadChunk(const int chunkhandle, const int offset,
                                 const int length) {
  // Data we are sending to the server.
  ReadChunkRequest request;
  request.set_chunkhandle(chunkhandle);
  request.set_offset(offset);
  request.set_length(length);

  // Container for the data we expect from the server.
  ReadChunkReply reply;

  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context;

  // The actual RPC.
  Status status = stub_->ReadChunk(&context, request, &reply);

  // Act upon its status.
  if (status.ok()) {
    if (reply.bytes_read() == 0) {
      return "ReadChunk failed";
    }
    return reply.data();
  } else {
    return "RPC failed";
  }
}


std::string GFSClient::WriteChunk(const int chunkhandle, const std::string data,
                                const int offset) {
  // Data we are sending to the server.
  WriteChunkRequest request;
  request.set_chunkhandle(chunkhandle);
  request.set_data(data);
  request.set_offset(offset);

  // Container for the data we expect from the server.
  WriteChunkReply reply;

  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context;

  // The actual RPC.
  Status status = stub_->WriteChunk(&context, request, &reply);

  // Act upon its status.
  if (status.ok()) {
    std::cout << "Write Chunk written_bytes = " << reply.bytes_written() << \
              std::endl;
    return "RPC succeeded";
  } else {
    return "RPC failed";
  }
}

void GFSClient::FindLeaseHolder(const std::string& filename, int64_t chunk_id) {
  FindLeaseHolderRequest request;
  request.set_filename(filename);
  request.set_chunk_index(chunk_id);

  FindLeaseHolderReply reply;
  ClientContext context;
  Status status = stub_master_->FindLeaseHolder(&context, request, &reply);
  if (status.ok()) {
    std::cout << "FindLeaseHolder file " << filename << " chunk id " << chunk_id
              << " got chunkhandle " << reply.chunkhandle() << std::endl;
  } else {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
  }
}

void GFSClient::FindMatchingFiles(const std::string& prefix) {
  FindMatchingFilesRequest request;
  request.set_prefix(prefix);

  FindMatchingFilesReply reply;
  ClientContext context;
  Status status = stub_master_->FindMatchingFiles(&context, request, &reply);
  if (status.ok()) {
    for (const auto& file_metadata : reply.files()) {
      std::cout << "FindMatchingFiles filename " << file_metadata.filename() << std::endl;
    }
  } else {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
  }
}
