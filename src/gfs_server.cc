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
#include <csignal>

#include <grpc++/grpc++.h>
#include <google/protobuf/timestamp.pb.h>
#include "gfs.grpc.pb.h"
#include "gfs_server.h"

GFSServiceImpl::GFSServiceImpl(std::string path, std::string server_address,
                               std::string master_address) {
  this->full_path = path;
  this->location_me = server_address;
  this->version_number = 1;
  this->am_i_dead = false;
  this->metadata_file = "metadata" + location_me;
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

  heartbeat_thread = std::thread(std::bind(
      &GFSServiceImpl::ServerMasterHeartbeat, this));
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

  if ((length + offset > CHUNK_SIZE_IN_BYTES)) {
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

  if ((length + offset > CHUNK_SIZE_IN_BYTES)) {
    std::cout << "Write exceeds chunk size: " << length + offset << std::endl;
    return 0;
  }

  // Open in r/w if it exists; Otherwise it's a new file
  std::ofstream outfile(filename.c_str(), std::ios::in | std::ios::out | std::ios::binary);
  if (!outfile.is_open())
  {
    outfile.clear();
    outfile.open(filename.c_str(), std::ios::out | std::ios::binary);
    assert(offset == 0);
  }
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
  request.set_padded_chunk(wc_info.padded_chunk);

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
  wc_info.padded_chunk = request->padded_chunk();

  if (wc_info.padded_chunk) {
    if(!PadChunk(wc_info)) {
      return Status(grpc::INTERNAL, "Couldn't complete PadChunk");
    }
  }

  bytes_written = PerformLocalWriteChunk(wc_info);
  reply->set_bytes_written(bytes_written);

  if (bytes_written) {
    ReportChunkInfo(wc_info.chunkhandle);
  }

  return Status::OK;
}

Status GFSServiceImpl::WriteChunk(ServerContext* context,
                                  const WriteChunkRequest* request,
                                  WriteChunkReply* reply) {
  int bytes_written;
  WriteChunkInfo wc_info;

  std::lock_guard<std::mutex> guard(write_mutex);
  std::cout << "Got server WriteChunk for chunkhandle = " << \
            request->chunkhandle() << std::endl;

  wc_info.client_id = request->client_id();
  wc_info.timestamp = request->timestamp();
  wc_info.chunkhandle = request->chunkhandle();
  wc_info.offset = request->offset();
  wc_info.padded_chunk = false;

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
    ReportChunkInfo(wc_info.chunkhandle);
  }

  return Status::OK;
}

// PadChunk replaces the buffercache data to 0s of length remaining in the chunk
bool GFSServiceImpl::PadChunk(const WriteChunkInfo& wc_info)
{
  ChunkId chunk_id;
  chunk_id.client_id = wc_info.client_id;
  Timestamp ts = wc_info.timestamp;
  chunk_id.timestamp.tv_sec = ts.seconds();
  chunk_id.timestamp.tv_usec = ts.nanos()/1000;
  int offset = wc_info.offset;

  std::lock_guard<std::mutex> guard(buffercache_mutex);

  if (buffercache.find(chunk_id) == buffercache.end()) {
    std::cout << "Chunk data doesn't exists in buffercache" << std::endl;
    return false;
  }

  std::string padding_data(CHUNK_SIZE_IN_BYTES - offset, '0');
  std::cout << "padding_data.len = " << padding_data.length() << std::endl;
  buffercache[chunk_id] = padding_data;

  return true;
}

Status GFSServiceImpl::Append(ServerContext* context,
                              const AppendRequest* request,
                              AppendReply* reply) {
  int bytes_written, length, existing_length = 0;
  WriteChunkInfo wc_info;

  std::lock_guard<std::mutex> guard(write_mutex);

  std::cout << "Got server Append for chunkhandle = " << \
            request->chunkhandle() << std::endl;

  wc_info.client_id = request->client_id();
  wc_info.timestamp = request->timestamp();
  wc_info.chunkhandle = request->chunkhandle();
  wc_info.padded_chunk = false;
  length = request->length();

  // TODO: This length can be in an in-memory structure and avoid us from having
  // to read the file
  std::string filename = this->full_path + "/" + \
                         std::to_string(wc_info.chunkhandle);
  std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);
  if (infile) {
    assert(infile.is_open() != 0);
    infile.seekg(0, std::ios::end);
    existing_length = infile.tellg();
    infile.seekg(0, std::ios::beg);
    infile.close();
  }

  wc_info.offset = existing_length;
  if (existing_length + length > CHUNK_SIZE_IN_BYTES) {
    if(!PadChunk(wc_info)) {
      return Status(grpc::INTERNAL,
                    "Couldn't complete Append. Pad Chunk failed");
    }
    wc_info.padded_chunk = true;
  }

  bytes_written = PerformLocalWriteChunk(wc_info);

  // If the local write succeeded, send SerializedWrites to backups
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
    ReportChunkInfo(wc_info.chunkhandle);
  }

  if (!bytes_written) {
    return Status(grpc::INTERNAL, "Couldn't complete Append");
  }

  reply->set_bytes_written(bytes_written);
  reply->set_offset(existing_length);

  if (wc_info.padded_chunk) {
    return Status(grpc::RESOURCE_EXHAUSTED, "Reached end of chunk");
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
            chunk_id.client_id << /*" and data = " << data <<*/ std::endl;

  std::lock_guard<std::mutex> guard(buffercache_mutex);

  if (buffercache.find(chunk_id) != buffercache.end()) {
    std::cout << "Chunk data already exists" << std::endl;
  }

  buffercache[chunk_id] = data;
  return Status::OK;
}

void GFSServiceImpl::ReportChunkInfo(int chunkhandle) {
  // Acquire the metadata mutex
  std::lock_guard<std::mutex> guard(metadata_mutex);

  // Check if we already have a recent write to the chunkhandle; if not write to
  // the metadata map
  if (metadata.find(chunkhandle) == metadata.end()) {
    metadata[chunkhandle] = version_number; // TODO: implement version
  }
}

// This function takes the in-memory metadata and sends it over to the Master.
// If there haven't been any writes in the last interval, it still sends an
// empty Heartbeat RPC so that the Master knows that the Server is alive
void GFSServiceImpl::ServerMasterHeartbeat() {
  int key, value;
  std::string filename = this->full_path + "/" + \
                         this->metadata_file;
  std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);

  // At startup, check whether we have a metadata file and populate the metadata
  // map with it.
  if (!infile.is_open()) {
    std::cout << "There's no metadata file for reading: " << filename
              << std::endl;
  } else {
    metadata_mutex.lock();
    while (infile >> key >> value) {
      metadata[key] = value;
    }
    metadata_mutex.unlock();
    infile.close();
  }

  while (true) {
    //std::cout << "Heartbeat thread woke up" << std::endl;
    am_i_dead_mutex.lock();
    if (am_i_dead) {
      break;
    }
    am_i_dead_mutex.unlock();

    HeartbeatRequest request;
    HeartbeatReply reply;
    ClientContext context;

    if (metadata.size() != 0) {
      // Acquire the metadata mutex
      metadata_mutex.lock();

      for (auto& ch : metadata) {
        auto *chunk_info = request.add_chunks();
        chunk_info->set_chunkhandle(ch.first);
        std::cout << "chunkhandle metadata: " << ch.first << std::endl;

        // Also write metadata to disk for crash recovery
        std::string filename = this->full_path + "/" + \
                               this->metadata_file;

        std::ofstream outfile(filename.c_str(), std::ios::app | std::ios::binary);
        if (!outfile.is_open()) {
          std::cout << "can't open file for writing: " << filename << std::endl;
          continue;
        }
        outfile << ch.first << " " << ch.second << "\n";
        outfile.close();
      }
      metadata.clear();
      metadata_mutex.unlock();
    }
    request.set_location(location_me);

    Status status = stub_master->Heartbeat(&context, request, &reply);
    if (status.ok()) {
     // std::cout << "New chunkhandle hearbeat sent " << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_DURATION_SECONDS));
  }
}

// ReplicateChunks is an RPC sent by Master to Chunkserver to send chunkhandles
// to another Chunkserver as part of re-replication
Status GFSServiceImpl::ReplicateChunks(ServerContext* context,
                                  const ReplicateChunksRequest* request,
                                  ReplicateChunksReply* reply) {
  //  std::lock_guard<std::mutex> guard(mutex);
  std::string location = request->location();
  std::cout << "Got replication request to location: " << request->location()
            << std::endl;

  CopyChunksRequest req;
  CopyChunksReply rep;
  ClientContext cont;

  // Create CopyChunksRequest -- (chunkhandle, data)
  for (const auto& chunk : request->chunks()) {
    // Read data from chunk.chunkhandle()
    std::string filename = this->full_path + "/" + \
                           std::to_string(chunk.chunkhandle());
    std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);
    if (!infile.is_open()) {
      std::cout << "can't open file for reading: " << filename << std::endl;
    } else {
      infile.seekg(0, std::ios::end);
      int length = infile.tellg();
      infile.seekg(0, std::ios::beg);

      std::string data(length, ' ');
      infile.read(&data[0], length);
      if (infile.gcount() != length) {
        std::cout << "couldn't read " << length << " bytes out of " <<
                  chunk.chunkhandle() << std::endl;
      }
      infile.close();
      auto *chunk_info = req.add_chunks();
      chunk_info->set_chunkhandle(chunk.chunkhandle());
      chunk_info->set_data(data);
      std::cout << "added chunk_info for " << chunk.chunkhandle() <<
                /*" and data: " << data <<*/ std::endl;
    }
  }

  // Create a connection to replica ChunkServer
  std::unique_ptr<gfs::GFS::Stub> stub = gfs::GFS::NewStub(
      grpc::CreateChannel(location, grpc::InsecureChannelCredentials()));

  Status status = stub->CopyChunks(&cont, req, &rep);

  if (status.ok()) {
    std::cout << "Copy request succeeded from: " << location_me << " to: " <<
              location << std::endl;
    return Status::OK;
  } else {
    std::cout << "Copy request failed from: " << location_me << " to: " <<
              location << std::endl;
    return Status(grpc::INTERNAL, "CopyRequest failed to backup replica.");
  }
}

Status GFSServiceImpl::CopyChunks(ServerContext* context,
                                  const CopyChunksRequest* request,
                                  CopyChunksReply* reply) {
  // Directly write to disk and add it to ReportChunkInfo so that master is sent
  // this info in the next Heartbeat
  for (const auto& chunk : request->chunks()) {
    std::string filename = this->full_path + "/" + \
                           std::to_string(chunk.chunkhandle());
    std::string data = chunk.data();
    int length = data.length();

    if ((length > CHUNK_SIZE_IN_BYTES)) {
      std::cout << "Write exceeds chunk size: " << length << std::endl;
      return Status(grpc::FAILED_PRECONDITION, "Write length > CHUNK_SIZE");
    }

    std::ofstream outfile(filename.c_str(), std::ios::out | std::ios::binary);
    if (!outfile.is_open()) {
      std::cout << "can't open file for writing: " << filename << std::endl;
      return Status(grpc::NOT_FOUND, "Can't open file for writing");
    } else {
      outfile.seekp(0, std::ios::beg);
      outfile.write(data.c_str(), length);
      outfile.close();
    }
    ReportChunkInfo(chunk.chunkhandle());
  }
  return Status::OK;
}

Status GFSServiceImpl::DeleteChunks(ServerContext* context,
                                    const DeleteChunksRequest* request,
                                    DeleteChunksReply* reply) {
  for (int64_t chunkhandle : request->chunkhandles()) {
    // TODO: Actually perform deletion.
    std::cout << "Got DeleteChunks request for " << chunkhandle << std::endl;
  }
  return Status(grpc::UNIMPLEMENTED, "Not implemented.");
}

GFSServiceImpl::~GFSServiceImpl() {
  // Signal thread to shutdown.
  am_i_dead_mutex.lock();
  am_i_dead = true;
  am_i_dead_mutex.unlock();

  // Wait for thread to exit.
  heartbeat_thread.join();
}

std::unique_ptr<Server> server;

void HandleTerminate(int signal) {
  if (server) {
    std::cout << "Shutting down." << std::endl;
    server->Shutdown();
  }
}

void RunServer(std::string master_address, std::string path,
               std::string server_address) {
  GFSServiceImpl service(path, server_address, master_address);

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.SetMaxReceiveMessageSize(1024*1024*100);
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  server = builder.BuildAndStart();
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
  std::signal(SIGINT, HandleTerminate);
  std::signal(SIGTERM, HandleTerminate);

  RunServer(argv[1], argv[2], argv[3]);
  return 0;
}
