#include <iostream>
#include <memory>
#include <string>
#include <set>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

class GFSClient {
 public:
  GFSClient(std::shared_ptr<grpc::Channel> channel);

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
  int Read(char *buf, const std::string& filename, onst int offset);

  // Writes the byte array content to the file with an off-set. Returns the
  // number of bytes it reads.
  bool Write(char *buf, const std::string& filename, onst int offset);
  
  // Appends the byte array to a file. Returns the off-set that the content resides in.
  int Append(har *buf, const std::string& filename);

private:
  // Client ReadChunk implementation
  std::string ReadChunk(const int chunkhandle);

  // Client WriteChunk implementation
  std::string WriteChunk(const int chunkhandle, const std::string data);

  std::unique_ptr<gfs::GFS::Stub> stub_;
};