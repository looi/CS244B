#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <inttypes.h>

#include "sqlite3.h"
#include "gfs_common.h"

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
  Status MoveFile(ServerContext* context,
                  const MoveFileRequest* request,
                  MoveFileReply* reply) override;
  Status DeleteFile(ServerContext* context,
                    const DeleteFileRequest* request,
                    DeleteFileReply* reply) override;
  Status Heartbeat(ServerContext* context,
                   const HeartbeatRequest* request,
                   HeartbeatReply* response) override;

 private:
  struct ChunkLocation {
    std::string location; // "ip:port"
    int64_t version;
  };

  // Periodically re-replicates data when chunkservers fail.
  void RereplicationThread();
  // Gets all chunkhandles from SQLite.
  std::set<int64_t> GetAllChunkhandlesFromDb();
  // Gets file id from SQLite or -1 if does not exist.
  int64_t GetFileId(const std::string& filename);
  // Gets chunkhandle from SQLite or -1 if does not exist.
  int64_t GetChunkhandle(int64_t file_id, int64_t chunk_index);
  // Gets locations for chunkhandle. First in the list is primary.
  std::vector<std::string> GetLocations(int64_t chunkhandle,
                                        bool new_chunk);
  // Helper function to re-replicate chunk if necessary.
  void RereplicateChunk(int64_t chunkhandle,
                        std::vector<ChunkLocation>* locations);
  void ThrowIfSqliteFailed(int rc);

  sqlite3 *db_;
  // Map from chunkhandle to location.
  // The first entry in the vector is the primary.
  std::map<int64_t, std::vector<ChunkLocation>> chunk_locations_;

  struct ChunkServer {
    time_t lease_expiry;
    std::unique_ptr<gfs::GFS::Stub> stub;
  };
  // Map from "ip:port" to chunkserver info.
  std::map<std::string, ChunkServer> chunk_servers_;

  std::mutex mutex_;
  bool shutdown_ = false;
  std::thread rereplication_thread_;
};
