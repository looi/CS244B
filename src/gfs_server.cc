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

using google::protobuf::Timestamp;

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

Status GFSServiceImpl::WriteChunk(ServerContext* context,
                                  const WriteChunkRequest* request,
                                  WriteChunkReply* reply) {
  ChunkId chunk_id;
  chunk_id.client_id = request->client_id();
  Timestamp ts = request->timestamp();
  chunk_id.timestamp.tv_sec = ts.seconds();
  chunk_id.timestamp.tv_usec = ts.nanos()/1000;
  int chunkhandle = request->chunkhandle();
  int offset = request->offset();

  std::cout << "Got server WriteChunk for chunkhandle = " << \
            request->chunkhandle() << std::endl;

  std::lock_guard<std::mutex> guard(buffercache_mutex);

  if (buffercache.find(chunk_id) == buffercache.end()) {
    std::cout << "Chunk data doesn't exists in buffercache" << std::endl;
    reply->set_bytes_written(0);
    return Status::OK;
  }

  std::string data = buffercache[chunk_id];
  int length = data.length();
  std::string filename = this->full_path + "/" + \
                         std::to_string(chunkhandle);

  if ((length + offset >= CHUNK_SIZE_IN_BYTES)) {
    std::cout << "Write exceeds chunk size: " << length + offset << std::endl;
    reply->set_bytes_written(0);
    return Status::OK;
  }

  std::ofstream outfile(filename.c_str(), std::ios::out | std::ios::binary);
  if (!outfile.is_open()) {
    std::cout << "can't open file for writing: " << filename << std::endl;
    reply->set_bytes_written(0);
  } else {
    outfile.seekp(offset, std::ios::beg);
    outfile.write(data.c_str(), length);
    outfile.close();
    reply->set_bytes_written(length);

    // Write succeeded, so we can remove the buffercache entry
    buffercache.erase(chunk_id);
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
