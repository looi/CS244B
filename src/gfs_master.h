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
using gfs::GFSMaster;
using gfs::FindMatchingFilesRequest;
using gfs::FindMatchingFilesReply;
using gfs::HeartbeatRequest;
using gfs::HeartbeatReply;

// Logic and data behind the server's behavior.
class GFSMasterImpl final : public GFSMaster::Service {
 public:
  GFSMasterImpl(std::string sqlite_db_path);
  ~GFSMasterImpl();
  Status FindLeaseHolder(ServerContext* context,
                         const FindLeaseHolderRequest* request,
                         FindLeaseHolderReply* reply) override;
  Status FindMatchingFiles(ServerContext* context,
                           const FindMatchingFilesRequest* request,
                           FindMatchingFilesReply* reply) override;
  Status Heartbeat(ServerContext* context,
                   const HeartbeatRequest* request,
                   HeartbeatReply* response) {
    return Status::OK;
  }

 private:
  void ThrowIfSqliteFailed(int rc);

  sqlite3 *db_;
};
