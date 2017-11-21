#include <iostream>
#include <memory>
#include <string>
#include <set>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

using grpc::Status;
using gfs::FindLeaseHolderReply;
using gfs::FindLocationsReply;

class GFSClient {
 public:
  GFSClient(std::shared_ptr<grpc::Channel> master_channel,
            int client_id)
    : stub_master_(gfs::GFSMaster::NewStub(master_channel))
    , client_id_(client_id) {
    }

  //Client API fucntions

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string ClientServerPing(const std::string& user, const std::string& cs);

  // Create a new file using absolute path. Returns true if success.
  bool Create(const std::string& filename);

  // Make a new directory using absolute path. Returns true if succeeded.
  bool Mkdir(const std::string& path);

  // List all files and directories under an absolute path.
  std::set<std::string> List(const std::string& path);

  // Delete a file (or a directory) given an absolute path. Returns true if
  // succeeded.
  bool Delete(const std::string& path);

  // Fills the string with contents with off-set in the file.
  Status Read(std::string* buf, const std::string& filename,
              const int offset, const int length);

  // Writes the string to the file with an off-set.
  Status Write(const std::string& buf, const std::string& filename, const int offset);

  // Appends the byte array to a file. Returns the off-set that the content resides in.
  int Append(char *buf, const std::string& filename);

  // Helper funtions (TODO: might need to move to private)

  // Client ReadChunk implementation
  std::string ReadChunk(const int chunkhandle, const int offset,
                        const int length, const std::string& location);

  // Client WriteChunk implementation
  std::string WriteChunk(const int chunkhandle, const std::string data,
                         const int offset, const std::vector<std::string>& locations,
                         const std::string& primary_location);

  bool PushData(gfs::GFS::Stub* stub, const std::string& data,
                const struct timeval timstamp);

  bool SendWriteToChunkServer(const int chunkhandle, const int offset,
                              const struct timeval timstamp,
                              const std::vector<std::string>& locations,
                              const std::string& primary_location);

  // Print all the file as for now
  void FindMatchingFiles(const std::string& prefix);

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
  int client_id_;
  std::string primary_;
  std::vector<std::string> chunkservers_;
};
