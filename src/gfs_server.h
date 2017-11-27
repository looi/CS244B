#include <string>
#include <mutex>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"
#include "gfs_common.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::GFS;
using gfs::GFSMaster;
using gfs::PingRequest;
using gfs::PingReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::SerializedWriteRequest;
using gfs::SerializedWriteReply;
using gfs::PushDataRequest;
using gfs::PushDataReply;
using gfs::HeartbeatRequest;
using gfs::HeartbeatReply;
using grpc::Channel;
using grpc::ClientContext;
using google::protobuf::Timestamp;

typedef struct ChunkId {
  int client_id;
  struct timeval timestamp;
} ChunkId;

typedef struct WriteChunkInfo {
  int client_id;
  Timestamp timestamp;
  int chunkhandle;
  int offset;
} WriteChunkInfo;

struct cmpChunkId {
  bool operator()(const ChunkId& c1, const ChunkId& c2) const {
    return ((c1.client_id < c2.client_id) ||
            (c1.client_id == c2.client_id &&
             ((c1.timestamp.tv_sec < c2.timestamp.tv_sec) ||
              (c1.timestamp.tv_sec == c2.timestamp.tv_sec &&
               c1.timestamp.tv_usec < c2.timestamp.tv_usec))));
  }
};

// Logic and data behind the server's behavior.
class GFSServiceImpl final : public GFS::Service {
public:
  GFSServiceImpl(std::string path, std::string server_address,
                 std::string master_address);

  Status ClientServerPing(ServerContext* context, const PingRequest* request,
                          PingReply* reply);

  Status ReadChunk(ServerContext* context, const ReadChunkRequest* request,
                   ReadChunkReply* reply);

  Status WriteChunk(ServerContext* context, const WriteChunkRequest* request,
                    WriteChunkReply* reply);

  Status PushData(ServerContext* context, const PushDataRequest* request,
                  PushDataReply* reply);

  Status Heartbeat(ServerContext* context, const PushDataRequest* request,
                  PushDataReply* reply);

  int PerformLocalWriteChunk(const WriteChunkInfo& wc_info);

  int SendSerializedWriteChunk(WriteChunkInfo& wc_info,
                               const std::string location);

  Status SerializedWrite(ServerContext* context,
                         const SerializedWriteRequest* request,
                         SerializedWriteReply* reply);

  void ReportChunkInfo(WriteChunkInfo& wc_info);

 private:
  std::unique_ptr<gfs::GFSMaster::Stub> stub_master;
  std::string full_path;
  std::string metadata_file;
  std::map<ChunkId, std::string, cmpChunkId> buffercache;
  std::map<int, int> metadata; // int chunkhandle, int version_no
  std::mutex buffercache_mutex;
  std::string location_me;
  int version_number;
};
