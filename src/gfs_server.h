#include <string>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

// TODO: Move this definition to a common header
#define CHUNK_SIZE_IN_BYTES 64 * (1 << 20) // 64MiB

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::PingRequest;
using gfs::PingReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::GFS;

// Logic and data behind the server's behavior.
class GFSServiceImpl final : public GFS::Service {
public:
  GFSServiceImpl(std::string path) {
    this->full_path = path;
  }

  Status ClientServerPing(ServerContext* context, const PingRequest* request,
                          PingReply* reply);

  Status ReadChunk(ServerContext* context, const ReadChunkRequest* request,
                   ReadChunkReply* reply);

  Status WriteChunk(ServerContext* context, const WriteChunkRequest* request,
                    WriteChunkReply* reply);
 private:
  std::string full_path;
};
