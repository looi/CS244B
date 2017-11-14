#include <string>
#include <mutex>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"

// TODO: Move this definition to a common header
#define CHUNK_SIZE_IN_BYTES 64 * (1 << 20) // 64MiB

typedef struct ChunkId {
  int client_id;
  struct timeval timestamp;
} ChunkId;

struct cmpChunkId {
  bool operator()(const ChunkId& c1, const ChunkId& c2) const {
    return ((c1.client_id < c2.client_id) ||
            (c1.client_id == c2.client_id &&
             ((c1.timestamp.tv_sec < c2.timestamp.tv_sec) ||
              (c1.timestamp.tv_sec == c2.timestamp.tv_sec &&
               c1.timestamp.tv_usec < c2.timestamp.tv_usec))));
  }
};

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
using gfs::PushDataRequest;
using gfs::PushDataReply;
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

  Status PushData(ServerContext* context, const PushDataRequest* request,
                  PushDataReply* reply);

 private:
  std::string full_path;
  std::string metadata_file;
  std::map<ChunkId, std::string, cmpChunkId> buffercache;
  std::mutex buffercache_mutex;
};
