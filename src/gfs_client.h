#include <iostream>
#include <memory>
#include <string>
#include <set>

#include "gfs_common.h"

class GFSClient {
 public:
  GFSClient(std::shared_ptr<grpc::Channel> master_channel,
            std::shared_ptr<grpc::Channel> benchmark_channel,
            int client_id)
    : stub_master_(gfs::GFSMaster::NewStub(master_channel))
    , stub_bm_(gfs::GFSBenchmark::NewStub(benchmark_channel))
    , client_id_(client_id) {
      std::cout << "Client initialized with id- " << client_id_ << '\n';
    }

  //Client API fucntions

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string ClientServerPing(const std::string& user, const std::string& cs);

  // Delete a file given an absolute path.
  Status Delete(const std::string& filename);

  // Moves a file given an absolute path.
  Status Move(const std::string& old_filename,
              const std::string& new_filename);

  // Fills the string with contents with off-set in the file.
  Status Read(std::string* buf, const std::string& filename,
              const long long offset, const int length);

  // Writes the string to the file with an off-set.
  Status Write(const std::string& buf, const std::string& filename, const long long offset);

  // Appends the byte array to a file. Returns the off-set that the content resides in.
  int Append(char *buf, const std::string& filename);

  // Client functions to report data to Benchmark Server
  void BMAddTestInfo(const std::string &);
  void BMAddData(long long duration_ms);

  // Helper funtions (TODO: might need to move to private)

  // Client ReadChunk implementation
  Status ReadChunk(const int chunkhandle, const long long offset,
                   const int length, const std::string& location, std::string *buf);

  // Client WriteChunk implementation
  std::string WriteChunk(const int chunkhandle, const std::string data,
                         const long long offset, const std::vector<std::string>& locations,
                         const std::string& primary_location);

  bool PushData(gfs::GFS::Stub* stub, const std::string& data,
                const struct timeval timstamp);

  bool SendWriteToChunkServer(const int chunkhandle, const long long offset,
                              const struct timeval timstamp,
                              const std::vector<std::string>& locations,
                              const std::string& primary_location);

  // Print all the file as for now
  void FindMatchingFiles(const std::string& prefix);

  // Gets number of chunks in the file
  int GetFileLength(const std::string& filename);

private:
  // Gets connection to chunkserver, opening a new one if necessary
  gfs::GFS::Stub* GetChunkserverStub(const std::string& location);

  // Get chunkhandle and locations of a file for reading
  Status FindLocations(FindLocationsReply *reply,
                       const std::string& filename,
                       int64_t chunk_index);

  // Get chunkhandle and locations of a file for writing,
  // create a chunk if the file is not found
  Status FindLeaseHolder(FindLeaseHolderReply *reply,
                         const std::string& filename,
                         int64_t chunk_index);

  std::map<std::string, std::unique_ptr<gfs::GFS::Stub>> stub_cs_;
  std::unique_ptr<gfs::GFSMaster::Stub> stub_master_;
  std::unique_ptr<gfs::GFSBenchmark::Stub> stub_bm_;
  int client_id_;
  std::string primary_;
  std::vector<std::string> chunkservers_;
};
