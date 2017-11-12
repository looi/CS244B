#include <iostream>
#include <memory>
#include <string>
#include <set>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

class GFSClient {
 public:
  GFSClient(std::shared_ptr<grpc::Channel> channel,
            std::shared_ptr<grpc::Channel> master_channel)
    : stub_(gfs::GFS::NewStub(channel))
    , stub_master_(gfs::GFSMaster::NewStub(master_channel)) {}

  //Client API fucntions

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string ClientServerPing(const std::string& user);

  // Create a new file using absolute path. Returns true if success.
  bool Create(const std::string& filename);

  // Make a new directory using absolute path. Returns true if succeeded.
  bool Mkdir(const std::string& path);

  // List all files and directories under an absolute path.
  std::set<std::string> List(const std::string& path);

  // Delete a file (or a directory) given an absolute path. Returns true if
  // succeeded.
  bool Delete(const std::string& path);

  // Fills the byte array with contents with off-set in the file. Returns the 
  // number of bytes it reads.
  int Read(char *buf, const std::string& filename, const int offset);

  // Writes the byte array content to the file with an off-set. Returns the
  // number of bytes it reads.
  bool Write(char *buf, const std::string& filename, const int offset);
  
  // Appends the byte array to a file. Returns the off-set that the content resides in.
  int Append(char *buf, const std::string& filename);



  // Helper funtions (TODO: might need to move to private)

  // Client ReadChunk implementation
  std::string ReadChunk(const int chunkhandle, const int offset,
                        const int length);

  // Client WriteChunk implementation
  std::string WriteChunk(const int chunkhandle, const std::string data,
                         const int offset);

  // Get chunkhandle of a file, create a chunk if the file is not found
  void FindLocations(const std::string& filename, int64_t chunk_id);

  // Print all the file as for now
  void FindMatchingFiles(const std::string& prefix);

private:
  // TODO: This stub_ needs to be replaced by one stub per chunkserver.
  std::unique_ptr<gfs::GFS::Stub> stub_;
  std::unique_ptr<gfs::GFSMaster::Stub> stub_master_;
};

