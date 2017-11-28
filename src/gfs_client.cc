#include "gfs_client.h"
#include "gfs_common.h"

#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <inttypes.h>
#include <sys/time.h>
#include <time.h>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"
#include <google/protobuf/timestamp.pb.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using gfs::AddConcurrentWriteClDataRequest;
using gfs::AddConcurrentWriteClDataReply;
using gfs::FindLeaseHolderRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindLocationsRequest;
using gfs::FindLocationsReply;
using gfs::FindMatchingFilesRequest;
using gfs::FindMatchingFilesReply;
using gfs::GetFileLengthRequest;
using gfs::GetFileLengthReply;
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

std::string FormatStatus(const Status& status) {
  if (status.ok()) {
    return "OK";
  }
  std::ostringstream ss;
  ss << "(" << status.error_code() << ": " << status.error_message() << ")";
  return ss.str();
}

// Client's main function
int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel("127.0.0.1:50052", grpc::InsecureChannelCredentials()),
      grpc::CreateChannel("127.0.0.1:88888", grpc::InsecureChannelCredentials()),
      42); // TODO: chose a better client_id

  gfs_client.FindMatchingFiles("a/test");
  std::string buf;
  Status status = gfs_client.Read(&buf, "a/test0.txt", 0, 10);
  std::cout << "Read status: " << FormatStatus(status)
            << " data: " << buf << std::endl;
  return 0;

  clock_t benchmark_t;
  for (int i = 0; i < 2; i++) {
    // int length;
    std::string data("new#data" + std::to_string(i));
    std::string filename("a/test" + std::to_string(i) + ".txt");
    benchmark_t = clock();
    Status status = gfs_client.Write(data, filename, 0);
    benchmark_t = clock() - benchmark_t;
    std::cout << "Write status: " << FormatStatus(status) << std::endl;

    // Pushing Write data to Benchmark Server
    gfs_client.BMAddConcurrentWriteClData(1, benchmark_t/(CLOCKS_PER_SEC/1000));

    std::string data2;
    status = gfs_client.Read(&data2, filename, 0, data.length());
    std::cout << "Read status: " << FormatStatus(status)
              << " data: " << data2 << std::endl;
    gfs_client.GetFileLength(filename);
  }
  gfs_client.FindMatchingFiles("a/test");
  return 0;
}

void GFSClient::BMAddConcurrentWriteClData(int client_number, int duration_ms) {
  AddConcurrentWriteClDataRequest request;
  request.set_client_number(client_number);
  request.set_duration_ms(duration_ms);
  ClientContext context;
  AddConcurrentWriteClDataReply reply;
  stub_bm_->AddConcurrentWriteClData(&context, request, &reply);
  std::cout << "Send data to BM got reply: " << reply.message();
}

std::string GFSClient::ClientServerPing(const std::string& user,
                                        const std::string& cs) {
  // Data we are sending to the server.
  PingRequest request;
  request.set_name(user);

  // Container for the data we expect from the server.
  PingReply reply;

  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context;

  // The actual RPC.
  Status status = stub_cs_[cs]->ClientServerPing(&context, request, &reply);

  // Act upon its status.
  if (status.ok()) {
    return reply.message();
  } else {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
    return "RPC failed";
  }
}

Status GFSClient::Read(std::string* buf, const std::string& filename,
                       const int offset, const int length) {
  int64_t chunk_index = offset / CHUNK_SIZE_IN_BYTES;
  int64_t chunk_offset = offset - (chunk_index * CHUNK_SIZE_IN_BYTES);
  FindLocationsReply find_locations_reply;
  Status status = FindLocations(&find_locations_reply, filename, chunk_index);
  if (!status.ok()) {
    return status;
  }
  // Pick first returned location
  const std::string& location = find_locations_reply.locations(0);
  // TODO: Handle ReadChunk error
  *buf = ReadChunk(find_locations_reply.chunkhandle(), chunk_offset, length, location);
  return Status::OK;
}

Status GFSClient::Write(const std::string& buf, const std::string& filename, const int offset) {
  int64_t chunk_index = offset / CHUNK_SIZE_IN_BYTES;
  int64_t chunk_offset = offset - (chunk_index * CHUNK_SIZE_IN_BYTES);
  FindLeaseHolderReply lease_holder;
  Status status = FindLeaseHolder(&lease_holder, filename, chunk_index);
  if (!status.ok()) {
    return status;
  }
  std::vector<std::string> locations;
  for (const auto& location : lease_holder.locations()) {
    locations.push_back(location);
  }
  // TODO: Handle WriteChunk error
  WriteChunk(lease_holder.chunkhandle(), buf, chunk_offset,
             locations, lease_holder.primary_location());
  return Status::OK;
}

std::string GFSClient::ReadChunk(const int chunkhandle, const int offset,
                                 const int length, const std::string& location) {
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
  Status status = GetChunkserverStub(location)->ReadChunk(&context, request, &reply);

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
                                  const int offset, const std::vector<std::string>& locations,
                                  const std::string& primary_location) {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  for (const auto& location : locations) {
    if (PushData(GetChunkserverStub(location), data, tv)) {
      std::cout << "PushData succeeded to chunk server " << location <<
                " for data = " << data << std::endl;
    } else {
      return "PushData RPC failed";
    }
  }

  if (SendWriteToChunkServer(chunkhandle, offset, tv, locations, primary_location)) {
    return "RPC succeeded";
  } else {
    return "Write RPC failed";
  }
}

bool GFSClient::PushData(gfs::GFS::Stub* stub,
                         const std::string& data,
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

bool GFSClient::SendWriteToChunkServer(const int chunkhandle, const int offset,
                                       const struct timeval tv,
                                       const std::vector<std::string>& locations,
                                       const std::string& primary_location) {
  WriteChunkRequest request;
  WriteChunkReply reply;
  ClientContext context;

  Timestamp timestamp;
  timestamp.set_seconds(tv.tv_sec);
  timestamp.set_nanos(tv.tv_usec * 1000);

  request.set_allocated_timestamp(&timestamp);
  request.set_client_id(client_id_);
  request.set_chunkhandle(chunkhandle);
  request.set_offset(offset);

  for (const auto& location : locations) {
    if (location != primary_location) {
      request.add_locations(location);
    }
  }

  Status status = GetChunkserverStub(primary_location)->WriteChunk(&context, request, &reply);
  request.release_timestamp();

  if (status.ok()) {
    std::cout << "Write Chunk written_bytes = " << reply.bytes_written() << \
              std::endl;
    return true;
  } else {
    std::cout << "Write Chunk failed " << std::endl;
    return false;
  }
}

Status GFSClient::FindLocations(FindLocationsReply *reply,
                                const std::string& filename,
                                int64_t chunk_index) {
  FindLocationsRequest request;
  request.set_filename(filename);
  request.set_chunk_index(chunk_index);

  ClientContext context;
  return stub_master_->FindLocations(&context, request, reply);
}

Status GFSClient::FindLeaseHolder(FindLeaseHolderReply *reply,
                                  const std::string& filename,
                                  int64_t chunk_index) {
  FindLeaseHolderRequest request;
  request.set_filename(filename);
  request.set_chunk_index(chunk_index);

  ClientContext context;
  return stub_master_->FindLeaseHolder(&context, request, reply);
}

void GFSClient::FindMatchingFiles(const std::string& prefix) {
  FindMatchingFilesRequest request;
  request.set_prefix(prefix);

  FindMatchingFilesReply reply;
  ClientContext context;
  Status status = stub_master_->FindMatchingFiles(&context, request, &reply);
  if (status.ok()) {
    std::cout << "FindMatchingFiles results: " << reply.files_size()
              << " files\n=======================================\n";
    for (const auto& file_metadata : reply.files()) {
      std::cout << file_metadata.filename() << std::endl;
    }
  } else {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
  }
}

void GFSClient::GetFileLength(const std::string& filename) {
  GetFileLengthRequest request;
  request.set_filename(filename);

  GetFileLengthReply reply;
  ClientContext context;
  Status status = stub_master_->GetFileLength(&context, request, &reply);
  if (status.ok()) {
    std::cout << "File " << filename << " num_chunks = " << reply.num_chunks() << std::endl;
  } else {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
  }
}

gfs::GFS::Stub* GFSClient::GetChunkserverStub(const std::string& location) {
  gfs::GFS::Stub* result;
  auto it = stub_cs_.find(location);
  if (it != stub_cs_.end()) {
    result = it->second.get();
  } else {
    auto stub = gfs::GFS::NewStub(
        grpc::CreateChannel(location, grpc::InsecureChannelCredentials()));
    result = stub.get();
    stub_cs_[location] = std::move(stub);
  }
  return result;
}
