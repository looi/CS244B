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
#include <sys/time.h>
#include <time.h>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"
#include <google/protobuf/timestamp.pb.h>

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
using gfs::PushDataRequest;
using gfs::PushDataReply;
using gfs::GFS;
using gfs::GFSMaster;
using google::protobuf::Timestamp;

// Client's main function
int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel("127.0.0.1:50051", grpc::InsecureChannelCredentials()),
      grpc::CreateChannel("127.0.0.1:50052", grpc::InsecureChannelCredentials()),
      42); // TODO: chose a better client_id
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

// Client wants to write a chunk at a particular offset. We already have the
// chunkhandle by contacting the master; This function will:
//  Create a Timestamp
//  Call PushData to all the ChunkServers
//  Call WriteChunk to the Primary ChunkServer (will contain list of secondary
//  ChunkServers)
//  TODO: logic to create connections to ChunkServers based on locations
std::string GFSClient::WriteChunk(const int chunkhandle, const std::string data,
                                  const int offset) {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  if (!PushData(stub_, data, tv)) {
    std::cout << "PushData succeeded for data = " << data << std::endl;
  } else {
    return "PushData RPC failed";
  }

  if (!SendWriteToChunkServer(stub_, chunkhandle, offset, tv)) {
    return "RPC succeeded";
  } else {
    return "Write RPC failed";
  }
}

bool GFSClient::PushData(std::unique_ptr<gfs::GFS::Stub> &stub,
                                const std::string data,
                                const struct timeval tv) {
  PushDataRequest request;
  PushDataReply reply;
  ClientContext context;

  Timestamp timestamp;
  timestamp.set_seconds(tv.tv_sec);
  timestamp.set_nanos(tv.tv_usec * 1000);

  request.set_allocated_timestamp(&timestamp);
  request.set_data(data);
  request.set_client_id(client_id_);

  Status status = stub->PushData(&context, request, &reply);
  request.release_timestamp();

  if (status.ok()) {
    std::cout << "PushData succeeded for data = " << data << std::endl;
    return true;
  } else {
    std::cout << "PushData failed for data = " << data << std::endl;
    return false;
  }
}

bool GFSClient::SendWriteToChunkServer(std::unique_ptr<gfs::GFS::Stub> &stub,
                      const int chunkhandle, const int offset,
                      const struct timeval tv) {
  WriteChunkRequest request;
  WriteChunkReply reply;
  ClientContext context;

  Timestamp timestamp;
  timestamp.set_seconds(tv.tv_sec);
  timestamp.set_nanos(tv.tv_usec * 1000);

  request.set_allocated_timestamp(&timestamp);
  request.set_client_id(client_id_);
  request.set_allocated_timestamp(&timestamp);
  request.set_chunkhandle(chunkhandle);
  request.set_offset(offset);

  Status status = stub->WriteChunk(&context, request, &reply);
  request.release_timestamp();

  if (status.ok()) {
    std::cout << "Write Chunk written_bytes = " << reply.bytes_written() << \
              std::endl;
    return true;
  } else {
    return false;
  }
}

void GFSClient::FindLeaseHolder(const std::string& filename, int64_t chunk_id) {
  FindLeaseHolderRequest request;
  request.set_filename(filename);
  request.set_chunk_index(chunk_index);

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
