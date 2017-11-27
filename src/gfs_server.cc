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
#include <inttypes.h>

#include <grpc++/grpc++.h>
#include <google/protobuf/timestamp.pb.h>
#include "gfs.grpc.pb.h"
#include "gfs_server.h"

GFSServiceImpl::GFSServiceImpl(std::string path, std::string server_address,
                               std::string master_address) {
  this->full_path = path;
  this->location_me = server_address;
  this->version_number = 1;
  stub_master = gfs::GFSMaster::NewStub(grpc::CreateChannel
                  (master_address,
                   grpc::InsecureChannelCredentials()));

  // Send initial heartbeat to master so that master knows about this server.
  HeartbeatRequest request;
  HeartbeatReply reply;
  ClientContext context;
  request.set_location(location_me);
  Status status = stub_master->Heartbeat(&context, request, &reply);
  if (status.ok()) {
    std::cout << "Successfully registered with master." << std::endl;
  } else {
    std::cout << "Failed to register with master." << std::endl;
  }
}

// Logic and data behind the server's behavior.
Status GFSServiceImpl::ClientServerPing(ServerContext* context,
                                        const PingRequest* request,
                                        PingReply* reply) {
  std::string prefix("Hello ");
  reply->set_message(prefix + request->name());
  return Status::OK;
}

Status GFSServiceImpl::ReadChunk(ServerContext* context,
                                 const ReadChunkRequest* request,
                                 ReadChunkReply* reply) {
  int chunkhandle = request->chunkhandle();
  int offset = request->offset();
  int length = request->length();
  std::string data(length, ' ');
  std::string filename = this->full_path + "/" + \
                         std::to_string(chunkhandle);

  if ((length + offset >= CHUNK_SIZE_IN_BYTES)) {
    std::cout << "Read exceeds chunk size: " << length + offset << std::endl;
    reply->set_bytes_read(0);
    return Status::OK;
  }

  std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);
  if (!infile.is_open()) {
    std::cout << "can't open file for reading: " << filename << std::endl;
    reply->set_bytes_read(0);
  } else {
    infile.seekg(offset, std::ios::beg);
    infile.read(&data[0], length);
    reply->set_bytes_read(infile.gcount());
    infile.close();
    reply->set_data(data);
  }

  return Status::OK;
}

int GFSServiceImpl::PerformLocalWriteChunk(const WriteChunkInfo& wc_info)
{
  ChunkId chunk_id;
  chunk_id.client_id = wc_info.client_id;
  Timestamp ts = wc_info.timestamp;
  chunk_id.timestamp.tv_sec = ts.seconds();
  chunk_id.timestamp.tv_usec = ts.nanos()/1000;
  int chunkhandle = wc_info.chunkhandle;
  int offset = wc_info.offset;

  std::lock_guard<std::mutex> guard(buffercache_mutex);

  if (buffercache.find(chunk_id) == buffercache.end()) {
    std::cout << "Chunk data doesn't exists in buffercache" << std::endl;
    return 0; // 0 bytes written
  }

  std::string data = buffercache[chunk_id];
  int length = data.length();
  std::string filename = this->full_path + "/" + \
                         std::to_string(chunkhandle);

  if ((length + offset >= CHUNK_SIZE_IN_BYTES)) {
    std::cout << "Write exceeds chunk size: " << length + offset << std::endl;
    return 0;
  }

  std::ofstream outfile(filename.c_str(), std::ios::out | std::ios::binary);
  if (!outfile.is_open()) {
    std::cout << "can't open file for writing: " << filename << std::endl;
    return 0;
  } else {
    outfile.seekp(offset, std::ios::beg);
    outfile.write(data.c_str(), length);
    outfile.close();

    // Write succeeded, so we can remove the buffercache entry
    buffercache.erase(chunk_id);
    return length;
  }
}

int GFSServiceImpl::SendSerializedWriteChunk(WriteChunkInfo& wc_info,
                                             const std::string location) {

  SerializedWriteRequest request;
  SerializedWriteReply reply;
  ClientContext context;

  // Create a connection to replica ChunkServer
  std::unique_ptr<gfs::GFS::Stub> stub = gfs::GFS::NewStub(
      grpc::CreateChannel(location, grpc::InsecureChannelCredentials()));

  // Prepare Request -> Perform RPC -> Read reply -> Return
  request.set_client_id(wc_info.client_id);
  request.set_allocated_timestamp(&wc_info.timestamp);
  request.set_chunkhandle(wc_info.chunkhandle);
  request.set_offset(wc_info.offset);

  Status status = stub->SerializedWrite(&context, request, &reply);
  request.release_timestamp();

  if (status.ok()) {
    std::cout << "SerializedWrite bytes_written = " << reply.bytes_written() <<
              " at location: " << location << std::endl;
    return reply.bytes_written();
  } else {
    std::cout << "SerializedWrite failed at location: " << location << std::endl;
    return 0;
  }

}

Status GFSServiceImpl::SerializedWrite(ServerContext* context,
                                  const SerializedWriteRequest* request,
                                  SerializedWriteReply* reply) {
  WriteChunkInfo wc_info;
  int bytes_written;

  wc_info.client_id = request->client_id();
  wc_info.timestamp = request->timestamp();
  wc_info.chunkhandle = request->chunkhandle();
  wc_info.offset = request->offset();

  bytes_written = PerformLocalWriteChunk(wc_info);
  reply->set_bytes_written(bytes_written);

  if (bytes_written) {
    ReportChunkInfo(wc_info);
  }

  return Status::OK;
}

Status GFSServiceImpl::WriteChunk(ServerContext* context,
                                  const WriteChunkRequest* request,
                                  WriteChunkReply* reply) {
  int bytes_written;
  WriteChunkInfo wc_info;

  std::cout << "Got server WriteChunk for chunkhandle = " << \
            request->chunkhandle() << std::endl;

  wc_info.client_id = request->client_id();
  wc_info.timestamp = request->timestamp();
  wc_info.chunkhandle = request->chunkhandle();
  wc_info.offset = request->offset();

  bytes_written = PerformLocalWriteChunk(wc_info);

  // If the local write succeeded, send SerializedWrites to backups
  // TODO: Doesn't work with multiple clients. Implement version number
  if (bytes_written) {
    for (const auto& location : request->locations()) {
      std::cout << "CS location: " << location << std::endl;
      if (location == location_me)
        continue;
      if (!SendSerializedWriteChunk(wc_info, location)) {
        bytes_written = 0;
        std::cout << "SerializedWrite failed for location: " << location
                  << std::endl;
        break;
      }
    }
  }

  reply->set_bytes_written(bytes_written);

  if (bytes_written) {
    ReportChunkInfo(wc_info);
  }

  return Status::OK;
}

Status GFSServiceImpl::PushData(ServerContext* context,
                                  const PushDataRequest* request,
                                  PushDataReply* reply) {
  ChunkId chunk_id;
  chunk_id.client_id = request->client_id();
  Timestamp ts = request->timestamp();
  chunk_id.timestamp.tv_sec = ts.seconds();
  chunk_id.timestamp.tv_usec = ts.nanos()/1000;
  std::string data = request->data();

  std::cout << "Got server PushData for clientid = " << \
            chunk_id.client_id << " and data = " << data << std::endl;

  std::lock_guard<std::mutex> guard(buffercache_mutex);

  if (buffercache.find(chunk_id) != buffercache.end()) {
    std::cout << "Chunk data already exists" << std::endl;
  }

  buffercache[chunk_id] = data;
  return Status::OK;
}

void GFSServiceImpl::ReportChunkInfo(WriteChunkInfo& wc_info) {
  HeartbeatRequest request;
  HeartbeatReply reply;
  ClientContext context;

  if (metadata.find(wc_info.chunkhandle) != metadata.end()) {
    return;
  }
  metadata[wc_info.chunkhandle] = version_number; // TODO: implement version

  auto *chunk_info = request.add_chunks();
  chunk_info->set_chunkhandle(wc_info.chunkhandle);
  request.set_location(location_me);

  Status status = stub_master->Heartbeat(&context, request, &reply);
  if (status.ok()) {
    std::cout << "New chunkhandle hearbeat sent for: " << wc_info.chunkhandle
              << std::endl;
  }
}

void RunServer(std::string master_address, std::string path,
               std::string server_address) {
  GFSServiceImpl service(path, server_address, master_address);

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
  if (argc != 4) {
    std::cout << "Usage: ./gfs_server master_address (like IP:port) \
              <path_to_local_file_directory> \
              chunkserver_address (like IP:port)"
              << std::endl;
    return 1;
  }
  RunServer(argv[1], argv[2], argv[3]);
  return 0;
}
