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


 private:
  // Gets file id from SQLite or -1 if does not exist.
  int64_t GetFileId(const std::string& filename);
  // Gets chunkhandle from SQLite or -1 if does not exist.
  int64_t GetChunkhandle(int64_t file_id, int64_t chunk_index);
  // Gets locations for chunkhandle. First in the list is primary.
  std::vector<std::string> GetLocations(int64_t chuknhandle);
  void ThrowIfSqliteFailed(int rc);

  sqlite3 *db_;
};
