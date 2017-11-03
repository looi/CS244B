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
using gfs::ErrorCode;

// Logic and data behind the server's behavior.
class GFSMasterImpl final : public GFSMaster::Service {
 public:
  GFSMasterImpl(std::string sqlite_db_path) {
    int rc = sqlite3_open(sqlite_db_path.c_str(), &db_);
    if (rc != SQLITE_OK) {
      throw std::runtime_error(sqlite3_errmsg(db_));
    }

    // Use Write-Ahead Logging for sqlite (https://sqlite.org/wal.html)
    // WAL mode should be faster, so we could benchmark with WAL on and off.
    rc = sqlite3_exec(db_, "PRAGMA journal_mode=WAL", nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) {
      throw std::runtime_error(sqlite3_errmsg(db_));
    }
  }

  ~GFSMasterImpl() {
    sqlite3_close(db_);
  }

  Status GetChunkhandle(ServerContext* context, const GetChunkhandleRequest* request,
                        GetChunkhandleReply* reply) override {
    return Status::OK;
  }

  Status ListFiles(ServerContext* context, const ListFilesRequest* request,
                   ListFilesReply* reply) override {
    return Status::OK;
  }

 private:
  sqlite3 *db_;
};

void RunServer(std::string sqlite_db_path) {
  std::string server_address("127.0.0.1:50052");
  GFSMasterImpl service(sqlite_db_path);

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "Usage: gfs_master path_to_sqlite_database" << std::endl;
    return 1;
  }

  RunServer(argv[1]);

  return 0;
}
