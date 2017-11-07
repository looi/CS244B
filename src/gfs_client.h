#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

class GFSClient {
 public:
  GFSClient(std::shared_ptr<grpc::Channel> channel);

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string ClientServerPing(const std::string& user);

  // Client ReadChunk implementation
  std::string ReadChunk(const int chunkhandle);

  // Client WriteChunk implementation
  std::string WriteChunk(const int chunkhandle, const std::string data);

 private:
  std::unique_ptr<gfs::GFS::Stub> stub_;
};