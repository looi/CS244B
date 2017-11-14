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
using gfs::GetChunkhandleRequest;
using gfs::GetChunkhandleReply;
using gfs::GFSMaster;
using gfs::ListFilesRequest;
using gfs::ListFilesReply;

// Logic and data behind the server's behavior.
class GFSMasterImpl final : public GFSMaster::Service {
 public:
  GFSMasterImpl(std::string sqlite_db_path);
  ~GFSMasterImpl();
  Status GetChunkhandle(ServerContext* context,
		                    const GetChunkhandleRequest* request,
                        GetChunkhandleReply* reply) override;
  Status ListFiles(ServerContext* context,
		               const ListFilesRequest* request,
                   ListFilesReply* reply) override;

 private:
  void ThrowIfSqliteFailed(int rc);

  sqlite3 *db_;
};
