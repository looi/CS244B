#include "gfs_client.h"

#include <iostream>
#include <fstream>
#include <memory>
#include <string>
#include <sstream>
#include <inttypes.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>
#include <random>
#include <thread>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"
#include <google/protobuf/timestamp.pb.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using gfs::AddDataRequest;
using gfs::AddDataReply;
using gfs::ClientBMHandshakeRequest;
using gfs::ClientBMHandshakeReply;
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
using gfs::AppendRequest;
using gfs::AppendReply;
using gfs::PushDataRequest;
using gfs::PushDataReply;
using gfs::GFS;
using gfs::GFSMaster;
using google::protobuf::Timestamp;

void RunCLientSimple(int argc, char* argv[]);
void RunClientCommand(int argc, char* argv[]);
void RunClientBenchmark(int argc, char* argv[]);
void RunClientMasterBenchmark(int argc, char* argv[]);

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
    } else if (strcmp(argv[4], "MASTER_BENCHMARK") == 0) {
      RunClientMasterBenchmark(argc, argv);
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
            << "\tappend\t<filepath>\t<data>\n"
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
    }else if (cmd == "append") {
      std::string filepath, data;
      int offset;
      if (line_stream >> filepath >> data) {
        int64_t chunk_index = gfs_client.GetFileLength(filepath);
        offset = gfs_client.Append(data, filepath, chunk_index);
        std::cout << "Appended to offset: " << offset << std::endl;
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
  // Set up default parameters
  const int kWarmUpTime_sec = 5;
  int runtime_sec = 10;
  int client_id = 0;
  enum Operation {READ, WRITE, PING, APPEND};
  enum Method {SEQUENTIAL, RANDOM};
  enum Filesystem {GFS, LOCAL};
  Operation op = Operation::READ;
  Method mode = Method::RANDOM;
  Filesystem fs = Filesystem::GFS;
  int window_size = 4096;
  std::string filename = "a/benchmark.txt";
  std::vector<std::string> info = {"READ", "RANDOM", "4096", "GFS", "a/benchmark.txt"};

  // Parse benchmark cmd arguments
  if (argc > 5) {
    for (int i = 5; i < argc; ++i) {
      std::string arg = argv[i];
      if (arg == "--id") {
        client_id = std::stoi(argv[++i]);
      } else if ((arg == "-o") || (arg == "--operation")) {
        std::string operation = argv[++i];
        if (operation == "write") {
          op = Operation::WRITE;
          info[0] = "WRITE";
        } else if (operation == "ping") {
          op = Operation::PING;
          info[0] = "PING";
        } else if (operation == "append") {
          op = Operation::APPEND;
          info[0] = "APPEND";
        }
      } else if ((arg == "-m") || (arg == "--method")) {
        std::string method = argv[++i];
        if (method == "sequential") {
          mode = Method::SEQUENTIAL;
          info[1] = "SEQUENTIAL";
        }
      } else if ((arg == "-s") || (arg == "--size")) {
        window_size = std::stoi(argv[++i]);
        info[2] = std::to_string(window_size);
      } else if ((arg == "-t") || (arg == "--time")) {
        runtime_sec = std::stoi(argv[++i]);
      } else if ((arg == "-f") || (arg == "--fs")) {
        std::string new_fs = argv[++i];
        if (new_fs == "local") {
          fs = Filesystem::LOCAL;
          info[3] = "LOCAL";
        }
      } else if ((arg == "-n") || (arg == "--filename")) {
        filename = argv[++i];
        info[4] = filename;
      }
    }
  }

  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  GFSClient gfs_client(
      grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()),
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()),
      client_id);

  // Print and send test info to master
  std::string test_info = "Client[" + std::to_string(client_id) + "] "
                          + info[0] + info[1] + info[2];
  gfs_client.ClientBMHandshake(info[0], info[1], window_size);

  Status status;
  // Create a file and pad it to 1024-chunk size
  std::string init_data(CHUNK_SIZE_IN_BYTES, 'x');
  long long num_chunk = 25;
  if (fs == Filesystem::GFS) {
    long long write_offset = 0;
    if (gfs_client.GetFileLength(filename) != num_chunk) {
      for (int i = 0; i < num_chunk; i++) {
        status = gfs_client.Write(init_data, filename, write_offset);
        if (!status.ok())
          std::cout << FormatStatus(status);
        write_offset += init_data.size();
      }
    }
  } else { // fs == Filesystem::LOCAL
    // Truncate local file and write.
    std::ofstream of(filename, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
    for (int i = 0; i < num_chunk; i++) {
      of << init_data;
    }
  }

  // Run benchmark.
  std::string bm_data(window_size, 'x');
  long long bm_offset = 0;
  const long long kMaxOffset = num_chunk * CHUNK_SIZE_IN_BYTES;
  std::string buf;

  std::random_device rd;
  std::mt19937 gen(rd());struct timespec start, end;
  //benchmark_start_t = clock();
  clock_gettime(CLOCK_REALTIME, &start);
  clock_gettime(CLOCK_REALTIME, &end);
  while (end.tv_sec - start.tv_sec < kWarmUpTime_sec + runtime_sec) {
    // Check if read/write on the boundary of chunks
    long long chunk_id = bm_offset / CHUNK_SIZE_IN_BYTES;
    long long remain_in_last_chunk = bm_offset - (chunk_id * CHUNK_SIZE_IN_BYTES);
    if ((CHUNK_SIZE_IN_BYTES - remain_in_last_chunk) < window_size) {
      bm_offset = (chunk_id + 1) * CHUNK_SIZE_IN_BYTES - window_size;
    }

    std::cout << "bm_offset = " << bm_offset << std::endl;
    struct timespec bm_start, bm_end;
    clock_gettime(CLOCK_REALTIME, &bm_start);
    if (op == Operation::PING) {
      FindLocationsReply find_locations_reply;
      gfs_client.FindLocations(&find_locations_reply, filename, 0, true);
      std::string user("test");
      gfs_client.ClientServerPing(user, find_locations_reply.locations(0));
    } else if (op == Operation::READ) {
      status = gfs_client.Read(&buf, filename, bm_offset, window_size);
      if (fs == Filesystem::GFS) {
        status = gfs_client.Read(&buf, filename, bm_offset, window_size);
      } else { // fs == Filesystem::LOCAL
        std::ifstream infile(filename, std::ios_base::in | std::ios_base::binary);
        infile.seekg(bm_offset);
        std::string data(window_size, ' ');
        infile.read(&data[0], window_size);
        if (infile.gcount() != window_size) {
          std::cout << "Read " << infile.gcount() << ", expected " << window_size << std::endl;
        }
        status = Status::OK;
      }
    } else if (op == Operation::APPEND) {
        gfs_client.Append(bm_data, filename,
                          gfs_client.GetFileLength(filename) - 1);
    } else {
      if (fs == Filesystem::GFS) {
        status = gfs_client.Write(bm_data, filename, bm_offset);
      } else { // fs == Filesystem::LOCAL
        std::ofstream of(filename, std::ios_base::in | std::ios_base::out | std::ios_base::binary);
        of.seekp(bm_offset);
        of.write(bm_data.c_str(), window_size);
        status = Status::OK;
      }
    }
    clock_gettime(CLOCK_REALTIME, &bm_end);
    if (!status.ok())
      std::cout << FormatStatus(status);
    long long duration = 1e9 * (bm_end.tv_sec - bm_start.tv_sec) +
        bm_end.tv_nsec - bm_start.tv_nsec;

    if (end.tv_sec - start.tv_sec > kWarmUpTime_sec) {
      // Pushing stats data to Benchmark Server after warm-up
      gfs_client.BMAddData(duration);
      std::cout << "window size: " << window_size << " duration: " << duration
                << "; throughput (B/s) = " << (double) window_size/(duration*1e-9) << std::endl;
    }

    if (mode == Method::SEQUENTIAL) {
      bm_offset += window_size;
    } else {
      std::uniform_int_distribution<> dis(0, kMaxOffset - 1);
      bm_offset = dis(gen);
    }
    if ((bm_offset + window_size) > kMaxOffset)
      bm_offset = 0;
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

int64_t ElapsedNs(timespec const& start, timespec const& end) {
  return (1000000000LL*end.tv_sec + end.tv_nsec) -
         (1000000000LL*start.tv_sec + start.tv_nsec);
}

void RunClientMasterBenchmark(int argc, char* argv[]) {
  GFSClient gfs_client(
      grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()),
      grpc::CreateChannel(argv[2], grpc::InsecureChannelCredentials()),
      42); // TODO: chose a better client_id

  // Generate a random filename for this benchmark.
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis('a', 'z');
  std::string filename = "master_benchmark/";
  for (int i = 0; i < 5; i++) {
    filename += char(dis(gen));
  }
  std::cout << "Using filename " << filename << std::endl;

  int64_t benchmark_duration_ns = 5000000000;
  // First let's benchmark creating new chunks.
  timespec start, current;
  clock_gettime(CLOCK_REALTIME, &start);
  int chunk_index = 0;
  do {
    FindLeaseHolderReply reply;
    Status status = gfs_client.FindLeaseHolder(&reply, filename, chunk_index);
    chunk_index++;
    if (!status.ok()) {
      std::cout << "FindLeaseHolder failed: " << FormatStatus(status) << std::endl;
    }
    clock_gettime(CLOCK_REALTIME, &current);
  } while (ElapsedNs(start, current) < benchmark_duration_ns);
  double seconds = ElapsedNs(start, current) * 1e-9;
  double chunks_created_per_second = double(chunk_index) / seconds;
  std::cout << "Created " << chunks_created_per_second << " chunks per second ("
            << chunk_index << " in " << seconds << "s)." << std::endl;

  // Now let's benchmark FindLocations for existing chunk.
  clock_gettime(CLOCK_REALTIME, &start);
  int total_chunks = chunk_index;
  int find_locations_done = 0;
  chunk_index = 0;
  do {
    FindLocationsReply reply;
    // Make sure to disable caching.
    Status status = gfs_client.FindLocations(&reply, filename, chunk_index, false);
    // Loop through all chunks repeatedly.
    chunk_index = (chunk_index + 1) % total_chunks;
    if (!status.ok()) {
      std::cout << "FindLocations failed: " << FormatStatus(status) << std::endl;
    }
    find_locations_done++;
    clock_gettime(CLOCK_REALTIME, &current);
  } while (ElapsedNs(start, current) < benchmark_duration_ns);
  seconds = ElapsedNs(start, current) * 1e-9;
  double find_locations_per_second = double(find_locations_done) / seconds;
  std::cout << "Performed " << find_locations_per_second << " FindLocations per second ("
            << find_locations_done << " in " << seconds << "s)." << std::endl;
}

// Client class member function implimentation

void GFSClient::ClientBMHandshake(
  const std::string &operation, const std::string &method, int size) {
  ClientBMHandshakeRequest request;
  request.set_id(client_id_);
  request.set_operation(operation);
  request.set_method(method);
  request.set_size(size);
  ClientContext context;
  ClientBMHandshakeReply reply;
  stub_bm_->ClientBMHandshake(&context, request, &reply);
  //std::cout << "Send data to BM got reply: " << reply.message();
}

void GFSClient::BMAddData(long long duration) {
  AddDataRequest request;
  request.set_id(client_id_);
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

  Status status = GetChunkserverStub(cs)->ClientServerPing(&context, request, &reply);
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
  // Keep trying to read from a random chunkserver until successful.
  std::random_device rd;
  std::mt19937 gen(rd());
  for (int i = 0; i < MAX_CLIENT_RETRIES; i++) {
    // Only use FindLocations cache on the first try.
    bool use_cache = (i == 0);
    FindLocationsReply find_locations_reply;
    Status status = FindLocations(&find_locations_reply, filename, chunk_index, use_cache);
    if (!status.ok()) {
      return status;
    }
    if (find_locations_reply.locations_size() == 0) {
      return Status(grpc::NOT_FOUND, "Unable to find replicas for chunk.");
    }
    std::uniform_int_distribution<> dis(0, find_locations_reply.locations_size() - 1);
    const std::string& location = find_locations_reply.locations(dis(gen));
    status = ReadChunk(find_locations_reply.chunkhandle(), chunk_offset,
                              length, location, buf);
    if (status.ok()) {
      return status;
    }
    std::cout << "Tried to read chunkhandle " << find_locations_reply.chunkhandle()
              << " from " << location << " but read failed with error " << FormatStatus(status)
              << ". Retrying." << std::endl;
    // Wait 1 second before retrying.
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return Status(grpc::ABORTED, "ReadChunk failed too many times.");
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
  return WriteChunk(lease_holder.chunkhandle(), buf, chunk_offset,
                    locations, lease_holder.primary_location());
}

// Returns offset in file that data was appended to
int GFSClient::Append(const std::string& buf, const std::string& filename,
                      int chunk_index) {
  int offset = -1;

  // Check if data is > MAX_APPEND_SIZE_IN_BYTES
  if (buf.length() > MAX_APPEND_SIZE_IN_BYTES) {
    std::cout << "Append size (" << buf.length() << " is > " <<
              MAX_APPEND_SIZE_IN_BYTES << "MiB" << std::endl;
    return 0;
  }

  FindLeaseHolderReply lease_holder;
  Status status = FindLeaseHolder(&lease_holder, filename, chunk_index);
  if (!status.ok()) {
    std::cout << "FindLeaseHolder failed" << std::endl;
    return 0;
  }
  std::vector<std::string> locations;
  for (const auto& location : lease_holder.locations()) {
    locations.push_back(location);
  }

  struct timeval tv;
  gettimeofday(&tv, NULL);

  for (const auto& location : locations) {
    PushData(GetChunkserverStub(location), buf, tv);
  }

  AppendRequest request;
  AppendReply reply;
  ClientContext context;

  Timestamp timestamp;
  timestamp.set_seconds(tv.tv_sec);
  timestamp.set_nanos(tv.tv_usec * 1000);

  request.set_allocated_timestamp(&timestamp);
  request.set_client_id(client_id_);
  request.set_chunkhandle(lease_holder.chunkhandle());
  request.set_length(buf.length());

  for (const auto& location : locations) {
    if (location != lease_holder.primary_location()) {
      request.add_locations(location);
    }
  }

  status = GetChunkserverStub(lease_holder.primary_location())->Append(&context, request, &reply);
  request.release_timestamp();

  if (status.ok()) {
    offset = reply.offset();
    return offset;
  } else if (status.error_code() == grpc::RESOURCE_EXHAUSTED) {
    std::cout << "Reached end of chunk; We'll try another Append" << std::endl;
    offset = Append(buf, filename, ++chunk_index);
    if (offset != -1) {
      std::cout << "Append Succeeded in 2nd try; Offset = " << offset << std::endl;
    }
    return offset;
  } else {
    std::cout << "Append failure" << std::endl;
    return -1;
  }
  return offset;
}

Status GFSClient::ReadChunk(const int chunkhandle, const long long offset, const int length, const std::string& location, std::string *buf) {
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
Status GFSClient::WriteChunk(const int chunkhandle, const std::string data,
                             const long long offset, const std::vector<std::string>& locations,
                             const std::string& primary_location) {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  for (const auto& location : locations) {
    Status status = PushData(GetChunkserverStub(location), data, tv);
    if (!status.ok()) {
      return status;
    }
  }

  return SendWriteToChunkServer(chunkhandle, offset, tv, locations, primary_location);
}

Status GFSClient::PushData(gfs::GFS::Stub* stub,
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

  if (!status.ok()) {
    std::cout << "PushData failed; " << FormatStatus(status) << std::endl;
  }
  return status;
}

Status GFSClient::SendWriteToChunkServer(const int chunkhandle, const long long offset,
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
  } else {
    std::cout << "Write Chunk failed " << std::endl;
  }
  return status;
}

Status GFSClient::FindLocations(FindLocationsReply *reply,
                                const std::string& filename,
                                int64_t chunk_index,
                                bool use_cache) {
  auto cache_key = std::make_pair(filename, chunk_index);
  if (use_cache) {
    auto it = find_locations_cache_.find(cache_key);
    if (it != find_locations_cache_.end()) {
      *reply = it->second;
      return Status::OK;
    }
  }

  FindLocationsRequest request;
  request.set_filename(filename);
  request.set_chunk_index(chunk_index);

  ClientContext context;
  Status status = stub_master_->FindLocations(&context, request, reply);
  if (status.ok()) {
    find_locations_cache_[cache_key] = *reply;
  }
  return status;
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

int GFSClient::GetFileLength(const std::string& filename) {
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
  return reply.num_chunks();
}

gfs::GFS::Stub* GFSClient::GetChunkserverStub(const std::string& location) {
  gfs::GFS::Stub* result;
  grpc::ChannelArguments argument;
  argument.SetMaxReceiveMessageSize(100*1024*1024);
  auto it = stub_cs_.find(location);
  if (it != stub_cs_.end()) {
    result = it->second.get();
  } else {
    auto stub = gfs::GFS::NewStub(
        grpc::CreateCustomChannel(location, grpc::InsecureChannelCredentials(), argument));
    result = stub.get();
    stub_cs_[location] = std::move(stub);
  }
  return result;
}
