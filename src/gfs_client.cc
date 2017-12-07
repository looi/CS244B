#include "gfs_client.h"

#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <inttypes.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>
#include <random>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"
#include <google/protobuf/timestamp.pb.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using gfs::AddDataRequest;
using gfs::AddDataReply;
using gfs::AddTestInfoRequest;
using gfs::AddTestInfoReply;
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

void RunCLientSimple(int argc, char* argv[]);
void RunClientCommand(int argc, char* argv[]);
void RunClientBenchmark(int argc, char* argv[]);

// Client's main function
int main(int argc, char* argv[]) {
  if (argc >= 5 &&
      (strcmp(argv[3], "-m") == 0 || strcmp(argv[3], "--mode"))) {
    if (strcmp(argv[4], "SIMPLE") == 0) {
      std::cout << "Running client in SIMPLE mode" << std::endl;
      RunCLientSimple(argc, argv);
      return 0;
    } else if (strcmp(argv[4], "COMMAND") == 0) {
      std::cout << "Running client in COMMAND mode" << std::endl;
      RunClientCommand(argc, argv);
      return 0;
    } else if (strcmp(argv[4], "BENCHMARK") == 0) {
      RunClientBenchmark(argc, argv);
      return 0;
    }
  }
  
  std::cout << "Usage: " << argv[0] << " <master_location> <benchmark_location>"
            << " <option> <option_arguement> <additional_arguments>\n"
            << "Locations like IP:port"
            << "Options:\n"
            << "\t-h,--help\t\tShow this help message\n"
            << "\t-m,--mode MODE\tSpecify the client mode,"
            << " options are: SIMPLE, COMMAND, BENCHMARK"
            << std::endl;
  return 0;
}

// Helper functions of client main functions

void RunClientCommand(int argc, char* argv[]) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()),
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()),
      42); // TODO: chose a better client_id

  std::cout << "Usage: <command> <arg1> <arg2> <arg3>...\n"
            << "Options:\n"
            << "\tread\t<filepath>\t<offset>\t<length>\n"
            << "\twrite\t<filepath>\t<offset>\t<data>\n"
            << "\tls\t<prefix>\n"
            << "\tmv\t<filepath>\t<new_filepath>\n"
            << "\trm\t<filepath>\n"
            << "\tquit"
            << std::endl;
  
  while (true) {
    std::cout << "> ";
    // Read an entire line and then read tokens from the line.
    std::string line;
    std::getline(std::cin, line);
    std::istringstream line_stream(line);

    std::string cmd;
    line_stream >> cmd;
    if (cmd == "read") {
      std::string filepath;
      int offset, length;
      if (line_stream >> filepath >> offset >> length) {
        std::string buf;
        Status status = gfs_client.Read(&buf, filepath, offset, length);
        std::cout << "Read status: " << FormatStatus(status)
                  << " data: " << buf << std::endl;
        continue;
      }
    } else if (cmd == "write") {
      std::string filepath, data;
      int offset;
      if (line_stream >> filepath >> offset >> data) {
        Status status = gfs_client.Write(data, filepath, offset);
        std::cout << "Write status: " << FormatStatus(status) << std::endl;
        continue;
      }
    } else if (cmd == "ls") {
      std::string prefix;
      if (line_stream >> prefix) {
        gfs_client.FindMatchingFiles(prefix);
      } else {
        // No prefix given, list all files.
        gfs_client.FindMatchingFiles("");
      }
      continue;
    } else if (cmd == "mv") {
      std::string old_filename, new_filename;
      if (line_stream >> old_filename >> new_filename) {
        Status status = gfs_client.Move(old_filename, new_filename);
        std::cout << "Move status: " << FormatStatus(status) << std::endl;
        continue;
      }
    } else if (cmd == "rm") {
      std::string filename;
      if (line_stream >> filename) {
        Status status = gfs_client.Delete(filename);
        std::cout << "Delete status: " << FormatStatus(status) << std::endl;
        continue;
      }
    } else if (cmd == "quit") {
      break;
    }
    std::cout << "Invalid command." << std::endl;
  }
}

void RunClientBenchmark(int argc, char* argv[]) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()),
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()),
      42); // TODO: chose a better client_id

  const clock_t kWarmUpTime_sec = 5;
  const clock_t kTotalRuntime_sec = 10;

  // Parse benchmark cmd arguments
  enum Operation {READ, WRITE};
  enum Method {SEQUENCIAL, RANDOM};
  int window_size = 4000;
  Operation op = Operation::READ;
  Method mode = Method::SEQUENCIAL;
  std::string test_info;
  if (argc > 5) {
    for (int i = 5; i < argc; ++i) {
      std::string arg = argv[i];
      if ((arg == "-o") || (arg == "--operation")) {
        std::string operation = argv[i++];
        if (operation == "write") {
          op = Operation::WRITE;
          test_info = test_info + "write ";
        } else {
          test_info = test_info + "read ";
        }
      } else if ((arg == "-m") || (arg == "--method")) {
        std::string method = argv[i++];
        if (method == "sequencial") {
          mode = Method::SEQUENCIAL;
          test_info = test_info + "sequencial ";
        } else {
          test_info = test_info + "random ";
        }
      } else if ((arg == "-s") || (arg == "--size")) {
        std::string size = argv[i++];
        if (size == "big") {
          window_size = CHUNK_SIZE_IN_BYTES;
          test_info = test_info + "big_size = " + std::to_string(window_size);
        } else {
          test_info = test_info + "small_size = " + std::to_string(window_size);
        }
      }
    }
  }
  gfs_client.BMAddTestInfo(test_info);

  // Create a file and pad it to 1024-chunk size
  std::string filename = "a/benchmark.txt";
  std::string init_data(CHUNK_SIZE_IN_BYTES, 'x');
  long long num_chunck = 10;
  long long write_offset = 0;
  for (int i = 0; i < num_chunck; i++) {
    gfs_client.Write(init_data, filename, write_offset);
    write_offset += init_data.size();
  }

  // Run benchmark.
  const int kOffsetIncrement = window_size;
  std::string bm_data(window_size, 'x');
  long long bm_offset = 0;
  const long long kMaxOffset = num_chunck * CHUNK_SIZE_IN_BYTES;
  std::string buf;

  struct timespec start, end;
  //benchmark_start_t = clock();
  clock_gettime(CLOCK_REALTIME, &start);
  clock_gettime(CLOCK_REALTIME, &end);
  while (end.tv_sec - start.tv_sec < kWarmUpTime_sec + kTotalRuntime_sec) {
    struct timespec bm_start, bm_end;
    clock_gettime(CLOCK_REALTIME, &bm_start);
    if (op == Operation::READ) {
      Status status = gfs_client.Read(&buf, filename, bm_offset, window_size);
    } else {
      Status status = gfs_client.Write(bm_data, filename, bm_offset);
    }
    clock_gettime(CLOCK_REALTIME, &bm_end);
    long long duration = 1e9 * (bm_end.tv_sec - bm_start.tv_sec) + bm_end.tv_nsec - bm_start.tv_nsec;

    if (end.tv_sec - start.tv_sec > kWarmUpTime_sec) {
      // Pushing stats data to Benchmark Server after warm-up
      gfs_client.BMAddData(duration);
    }

    if (mode == Method::SEQUENCIAL) {
      write_offset += kOffsetIncrement;
    } else {
      double ratio = (rand() % 100) / 100;
      write_offset = floor(ratio * (kMaxOffset - CHUNK_SIZE_IN_BYTES));
    }
    clock_gettime(CLOCK_REALTIME, &end);
  }
}

void RunCLientSimple(int argc, char* argv[]) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()),
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()),
      42); // TODO: chose a better client_id

  gfs_client.FindMatchingFiles("a/test");
  std::string buf;
  Status status = gfs_client.Read(&buf, "a/test0.txt", 0, 10);
  std::cout << "Read status: " << FormatStatus(status)
            << " data: " << buf << std::endl;

  for (int i = 0; i < 2; i++) {
    // int length;
    std::string data("new#data" + std::to_string(i));
    std::string filename("a/test" + std::to_string(i) + ".txt");
    
    Status status = gfs_client.Write(data, filename, 0);
    std::cout << "Write status: " << FormatStatus(status) << std::endl;

    std::string data2;
    status = gfs_client.Read(&data2, filename, 0, data.length());
    std::cout << "Read status: " << FormatStatus(status)
              << " data: " << data2 << std::endl;
    gfs_client.GetFileLength(filename);
  }
  gfs_client.FindMatchingFiles("a/test");
}

// Client class member function implimentation

void GFSClient::BMAddTestInfo(const std::string &info) {
  AddTestInfoRequest request;
  request.set_info(info);
  ClientContext context;
  AddTestInfoReply reply;
  stub_bm_->AddTestInfo(&context, request, &reply);
  //std::cout << "Send data to BM got reply: " << reply.message();
}

void GFSClient::BMAddData(long long duration) {
  AddDataRequest request;
  request.set_duration(duration);
  ClientContext context;
  AddDataReply reply;
  stub_bm_->AddData(&context, request, &reply);
  //std::cout << "Send data to BM got reply: " << reply.message();
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

Status GFSClient::Delete(const std::string& filename) {
  DeleteFileRequest request;
  request.set_filename(filename);

  DeleteFileReply reply;
  ClientContext context;
  return stub_master_->DeleteFile(&context, request, &reply);
}

Status GFSClient::Move(const std::string& old_filename,
                       const std::string& new_filename) {
  MoveFileRequest request;
  request.set_old_filename(old_filename);
  request.set_new_filename(new_filename);

  MoveFileReply reply;
  ClientContext context;
  return stub_master_->MoveFile(&context, request, &reply);
}

Status GFSClient::Read(std::string* buf, const std::string& filename,
                       const long long offset, const int length) {
  int64_t chunk_index = offset / CHUNK_SIZE_IN_BYTES;
  int64_t chunk_offset = offset - (chunk_index * CHUNK_SIZE_IN_BYTES);
  FindLocationsReply find_locations_reply;
  Status status = FindLocations(&find_locations_reply, filename, chunk_index);
  if (!status.ok()) {
    return status;
  }
  if (find_locations_reply.locations_size() == 0) {
    return Status(grpc::NOT_FOUND, "Unable to find replicas for chunk.");
  }
  // Keep trying to read from a random chunkserver until successful.
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, find_locations_reply.locations_size() - 1);
  while (true) {
    const std::string& location = find_locations_reply.locations(dis(gen));
    Status status = ReadChunk(find_locations_reply.chunkhandle(), chunk_offset,
                              length, location, buf);
    if (status.ok()) {
      return status;
    }
    std::cout << "Tried to read chunkhandle " << find_locations_reply.chunkhandle()
              << " from " << location << " but read failed with error " << FormatStatus(status)
              << ". Retrying." << std::endl;
  }
}

Status GFSClient::Write(const std::string& buf, const std::string& filename, const long long offset) {
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

Status GFSClient::ReadChunk(const int chunkhandle, const long long offset,
                            const int length, const std::string& location, std::string *buf) {
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
      return Status(grpc::NOT_FOUND, "Data not found at chunkserver.");
    } else if (reply.bytes_read() != length) {
      std::cout << "Warning: ReadChunk read " << reply.bytes_read() << " bytes but asked for "
                << length << "." << std::endl;
    }
    *buf = reply.data();
  }
  return status;
}

// Client wants to write a chunk at a particular offset. We already have the
// chunkhandle by contacting the master; This function will:
//  Create a Timestamp
//  Call PushData to all the ChunkServers
//  Call WriteChunk to the Primary ChunkServer (will contain list of secondary
//  ChunkServers)
//  TODO: logic to create connections to ChunkServers based on locations
std::string GFSClient::WriteChunk(const int chunkhandle, const std::string data,
                                  const long long offset, const std::vector<std::string>& locations,
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

bool GFSClient::SendWriteToChunkServer(const int chunkhandle, const long long offset,
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
