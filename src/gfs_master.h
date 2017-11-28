#include <iostream>
#include <memory>
#include <string>
#include <inttypes.h>

#include <grpc++/grpc++.h>
#include "gfs.grpc.pb.h"
#include "sqlite3.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::FindLeaseHolderRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindLocationsRequest;
using gfs::FindLocationsReply;
using gfs::GFSMaster;
using gfs::FindMatchingFilesRequest;
using gfs::FindMatchingFilesReply;
using gfs::GetFileLengthRequest;
using gfs::GetFileLengthReply;
using gfs::HeartbeatRequest;
using gfs::HeartbeatReply;

// Logic and data behind the server's behavior.
class GFSMasterImpl final : public GFSMaster::Service {
 public:
  GFSMasterImpl(std::string sqlite_db_path);
  ~GFSMasterImpl();
  Status FindLocations(ServerContext* context,
                       const FindLocationsRequest* request,
                       FindLocationsReply* reply) override;
  Status FindLeaseHolder(ServerContext* context,
                         const FindLeaseHolderRequest* request,
                         FindLeaseHolderReply* reply) override;
  Status FindMatchingFiles(ServerContext* context,
                           const FindMatchingFilesRequest* request,
                           FindMatchingFilesReply* reply) override;
  Status GetFileLength(ServerContext* context,
                       const GetFileLengthRequest* request,
                       GetFileLengthReply* reply) override;
  Status Heartbeat(ServerContext* context,
                   const HeartbeatRequest* request,
                   HeartbeatReply* response);

 private:
  // Gets file id from SQLite or -1 if does not exist.
  int64_t GetFileId(const std::string& filename);
  // Gets chunkhandle from SQLite or -1 if does not exist.
  int64_t GetChunkhandle(int64_t file_id, int64_t chunk_index);
  // Gets locations for chunkhandle. First in the list is primary.
  std::vector<std::string> GetLocations(int64_t chuknhandle);
  void ThrowIfSqliteFailed(int rc);

  sqlite3 *db_;

  struct ChunkLocation {
    std::string location; // "ip:port"
    int64_t version;
  };
  // Map from chunkhandle to location.
  // The first entry in the vector is the primary.
  std::map<int64_t, std::vector<ChunkLocation>> chunk_locations_;

  struct ChunkServer {
    time_t lease_expiry;
  };
  // Map from "ip:port" to chunkserver info.
  std::map<std::string, ChunkServer> chunk_servers_;

  std::mutex mutex_;
};
