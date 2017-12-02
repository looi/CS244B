#include <algorithm>
#include <csignal>
#include <random>

#include "gfs_master.h"
#include "gfs_common.h"

const char *const DB_INIT_QUERIES[] = {
  // Use Write-Ahead Logging for sqlite (https://sqlite.org/wal.html)
  // WAL mode should be faster, so we could benchmark with WAL on and off.
  "PRAGMA journal_mode=WAL",

  // The file table maps each filename to a unique file ID.
  "CREATE TABLE IF NOT EXISTS file (file_id INTEGER PRIMARY KEY, filename TEXT NOT NULL)",
  // Enforce uniqueness of filename.
  "CREATE UNIQUE INDEX IF NOT EXISTS file_filename ON file (filename)",

  // The chunk table maps each chunkhandle to a file_id.
  // For each file, successive chunks have increasing chunkhandles.
  "CREATE TABLE IF NOT EXISTS chunk (chunkhandle INTEGER PRIMARY KEY, "
    "file_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL)",
  // Enforce uniqueness of (file_id, chunk_index).
  "CREATE UNIQUE INDEX IF NOT EXISTS chunk_file_id_chunk_index ON chunk (file_id, chunk_index)",
};

GFSMasterImpl::GFSMasterImpl(std::string sqlite_db_path) {
  ThrowIfSqliteFailed(sqlite3_open(sqlite_db_path.c_str(), &db_));

  for (const char *query : DB_INIT_QUERIES) {
    ThrowIfSqliteFailed(sqlite3_exec(db_, query, nullptr, nullptr, nullptr));
  }

  // Start rereplication thread.
  rereplication_thread_ = std::thread(std::bind(&GFSMasterImpl::RereplicationThread, this));
}

GFSMasterImpl::~GFSMasterImpl() {
  // Signal thread to shutdown.
  shutdown_ = true;

  // Wait for thread to exit.
  rereplication_thread_.join();

  sqlite3_close(db_);
}

Status GFSMasterImpl::FindLocations(ServerContext* context,
                                    const FindLocationsRequest* request,
                                    FindLocationsReply* reply) {
  const std::string& filename = request->filename();
  const int64_t chunk_index = request->chunk_index();

  // Get file id.
  int64_t file_id = GetFileId(filename);
  if (file_id == -1) {
    return Status(grpc::NOT_FOUND, "File does not exist.");
  }

  // Get chunkhandle.
  int64_t chunkhandle = GetChunkhandle(file_id, chunk_index);
  if (chunkhandle == -1) {
    return Status(grpc::NOT_FOUND, "Chunk index does not exist.");
  }

  // Get locations
  std::vector<std::string> locations = GetLocations(chunkhandle, false);
  if (locations.empty()) {
    return Status(grpc::NOT_FOUND, "Chunk exists but unable to find locations. Probably need to wait for heartbeat from chunkserver.");
  }
  
  reply->set_chunkhandle(chunkhandle);
  for (const auto& location : locations) {
    reply->add_locations(location);
  }
  return Status::OK;
}

Status GFSMasterImpl::FindLeaseHolder(ServerContext* context,
                                      const FindLeaseHolderRequest* request,
                                      FindLeaseHolderReply* reply) {
  const std::string& filename = request->filename();
  const int64_t chunk_index = request->chunk_index();

  // Try to insert file, ignoring if it already exists
  sqlite3_stmt *insert_file_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "INSERT OR IGNORE INTO file (filename) VALUES (?)",
      -1, &insert_file_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_text(insert_file_stmt,
      1, filename.c_str(), filename.length(), SQLITE_STATIC));
  ThrowIfSqliteFailed(sqlite3_step(insert_file_stmt));
  ThrowIfSqliteFailed(sqlite3_finalize(insert_file_stmt));

  // Get file id.
  int64_t file_id = GetFileId(filename);
  if (file_id == -1) {
    return Status(grpc::NOT_FOUND, "File does not exist.");
  }

  // Chunk index - 1 must exist already
  if (chunk_index > 1 && GetChunkhandle(file_id, chunk_index-1) == -1) {
    return Status(grpc::FAILED_PRECONDITION, "Chunk index - 1 does not exist.");
  }

  // Try to insert chunk, ignoring if it already exists
  sqlite3_stmt *insert_chunk_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "INSERT OR IGNORE INTO chunk (file_id, chunk_index) VALUES (?, ?)",
      -1, &insert_chunk_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_int64(insert_chunk_stmt, 1, file_id));
  ThrowIfSqliteFailed(sqlite3_bind_int64(insert_chunk_stmt, 2, chunk_index));
  ThrowIfSqliteFailed(sqlite3_step(insert_chunk_stmt));
  ThrowIfSqliteFailed(sqlite3_finalize(insert_chunk_stmt));
  bool new_chunk = sqlite3_changes(db_) > 0;

  // Get chunkhandle.
  int64_t chunkhandle = GetChunkhandle(file_id, chunk_index);
  if (chunkhandle == -1) {
    // This should never happen because the chunk was created if necessary.
    return Status(grpc::NOT_FOUND, "Chunk index does not exist.");
  }
  if (new_chunk) {
    std::cout << "Created new chunkhandle " << chunkhandle << std::endl;
  }

  // Get locations
  std::vector<std::string> locations = GetLocations(chunkhandle, new_chunk);
  if (locations.empty()) {
    return Status(grpc::NOT_FOUND, "Chunk exists but unable to find locations. Probably need to wait for heartbeat from chunkserver.");
  }
  
  reply->set_chunkhandle(chunkhandle);
  for (const auto& location : locations) {
    reply->add_locations(location);
  }
  reply->set_primary_location(locations.at(0));
  return Status::OK;
}

Status GFSMasterImpl::FindMatchingFiles(ServerContext* context,
                                        const FindMatchingFilesRequest* request,
                                        FindMatchingFilesReply* reply) {
  std::string like_query = request->prefix() + "%";

  // Get file list from sqlite.
  sqlite3_stmt *list_files_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "SELECT filename FROM file WHERE filename LIKE ? ORDER BY filename",
      -1, &list_files_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_text(list_files_stmt,
      1, like_query.c_str(), like_query.length(), SQLITE_STATIC));
  while (sqlite3_step(list_files_stmt) == SQLITE_ROW) {
    const unsigned char *filename_bytes = sqlite3_column_text(list_files_stmt, 0);
    int filename_length = sqlite3_column_bytes(list_files_stmt, 0);
    std::string filename(reinterpret_cast<const char*>(filename_bytes), filename_length);
    auto *file_metadata = reply->add_files();
    file_metadata->set_filename(filename);
  }
  ThrowIfSqliteFailed(sqlite3_finalize(list_files_stmt));
  return Status::OK;
}

Status GFSMasterImpl::GetFileLength(ServerContext* context,
                                    const GetFileLengthRequest* request,
                                    GetFileLengthReply* reply) {
  int64_t file_id = GetFileId(request->filename());
  if (file_id == -1) {
    return Status(grpc::NOT_FOUND, "File does not exist.");
  }

  // Get number of chunks from sqlite.
  sqlite3_stmt *select_count_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "SELECT COUNT(*) FROM chunk WHERE file_id=?",
      -1, &select_count_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_int64(select_count_stmt, 1, file_id));
  ThrowIfSqliteFailed(sqlite3_step(select_count_stmt));
  int64_t num_chunks = sqlite3_column_int64(select_count_stmt, 0);
  ThrowIfSqliteFailed(sqlite3_finalize(select_count_stmt));
  reply->set_num_chunks(num_chunks);
  return Status::OK;
}

Status GFSMasterImpl::Heartbeat(ServerContext* context,
                                const HeartbeatRequest* request,
                                HeartbeatReply* response) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!chunk_servers_.count(request->location())) {
    std::cout << "Found out about new chunkserver: " << request->location() << std::endl;
  }
  // Set new lease expiry for chunkserver.
  chunk_servers_[request->location()].lease_expiry = time(nullptr) + LEASE_DURATION_SECONDS;
  for (const auto& chunk : request->chunks()) {
    auto& locations = chunk_locations_[chunk.chunkhandle()];
    bool already_know = false;
    for (const auto& location : locations) {
      if (location.location == request->location()) {
        already_know = true;
        break;
      }
    }
    if (!already_know) {
      // If already_know is false, that means the master crashed and is now
      // re-learning chunk locations.
      // For now, the primary (first in locations vector) is arbitrarily assigned.
      // Actually, it should be based on whoever has the highest version.
      ChunkLocation location;
      location.location = request->location();
      location.version = 0; // TODO: implement versions.
      locations.push_back(location);
      std::cout << "Found out that chunkserver " << request->location()
                << " stores chunk " << chunk.chunkhandle() << std::endl;
    }
  }
  return Status::OK;
}

int64_t GFSMasterImpl::GetFileId(const std::string& filename) {
  sqlite3_stmt *select_file_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "SELECT file_id FROM file WHERE filename=?",
      -1, &select_file_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_text(select_file_stmt,
      1, filename.c_str(), filename.length(), SQLITE_STATIC));
  if (sqlite3_step(select_file_stmt) != SQLITE_ROW) return -1;
  int64_t file_id = sqlite3_column_int64(select_file_stmt, 0);
  ThrowIfSqliteFailed(sqlite3_finalize(select_file_stmt));
  return file_id;
}

int64_t GFSMasterImpl::GetChunkhandle(int64_t file_id, int64_t chunk_index) {
  sqlite3_stmt *select_chunk_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "SELECT chunkhandle FROM chunk WHERE file_id=? AND chunk_index=?",
      -1, &select_chunk_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_int64(select_chunk_stmt, 1, file_id));
  ThrowIfSqliteFailed(sqlite3_bind_int64(select_chunk_stmt, 2, chunk_index));
  if (sqlite3_step(select_chunk_stmt) != SQLITE_ROW) return -1;
  int64_t chunkhandle = sqlite3_column_int64(select_chunk_stmt, 0);
  ThrowIfSqliteFailed(sqlite3_finalize(select_chunk_stmt));
  return chunkhandle;
}

std::vector<std::string> GFSMasterImpl::GetLocations(int64_t chunkhandle,
                                                     bool new_chunk) {
  std::vector<std::string> result;
  std::lock_guard<std::mutex> guard(mutex_);
  auto& locations = chunk_locations_[chunkhandle];
  if (new_chunk) {
    // Only try to re-replicate for a new chunk.
    RereplicateChunk(chunkhandle, &locations);
  }
  for (const auto& chunk_location : locations) {
    result.push_back(chunk_location.location);
  }
  return result;
}

void GFSMasterImpl::RereplicateChunk(int64_t chunkhandle,
                                     std::vector<ChunkLocation>* locations) {
  if (locations->size() < NUM_CHUNKSERVER_REPLICAS) {
    if (chunk_servers_.size() >= NUM_CHUNKSERVER_REPLICAS) {
      // Randomly pick chunkservers to add.
      std::vector<std::string> all_locations;
      for (const auto& location : chunk_servers_) {
        all_locations.push_back(location.first);
      }
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(0, chunk_servers_.size() - 1);
      while (locations->size() < NUM_CHUNKSERVER_REPLICAS) {
        const std::string& try_location = all_locations[dis(gen)];
        bool already_in_list = false;
        for (const auto& location : *locations) {
          if (location.location == try_location) {
            already_in_list = true;
            break;
          }
        }
        if (!already_in_list) {
          ChunkLocation chunk_location;
          chunk_location.location = try_location;
          chunk_location.version = 0; // TODO: implement version
          locations->push_back(chunk_location);
          std::cout << "Added new location " << try_location
                    << " for chunkhandle " << chunkhandle << std::endl;
        }
      }
    } else {
      std::cout << "ERROR: Number of known chunkservers is less than "
                << NUM_CHUNKSERVER_REPLICAS << std::endl;
    }
  }
}

void GFSMasterImpl::ThrowIfSqliteFailed(int rc) {
  if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
    throw std::runtime_error(sqlite3_errmsg(db_));
  }
}

void GFSMasterImpl::RereplicationThread() {
  while (!shutdown_) {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      // Detect failed chunkservers.
      time_t current_time = time(nullptr);
      for (auto it = chunk_servers_.begin(); it != chunk_servers_.end(); ) {
        const std::string& location = it->first;
        const auto& chunk_server = it->second;
        if (chunk_server.lease_expiry < current_time) {
          std::cout << "Lease for chunkserver " << location << " expired." << std::endl;
          // Forget this chunkserver.
          it = chunk_servers_.erase(it);
        } else {
          // Chunkserver lease is ok.
          ++it;
        }
      }

      // Rereplicate chunks if necessary
      for (auto& it : chunk_locations_) {
        int64_t chunkhandle = it.first;
        auto& locations = it.second;
        for (auto location = locations.begin(); location != locations.end(); ) {
          if (chunk_servers_.count(location->location) == 0) {
            // This chunkserver's lease expired.
            location = locations.erase(location);
          } else {
            ++location;
          }
        }
        RereplicateChunk(chunkhandle, &locations);
      }
    }

    // Wait for 1 second, unless shutdown was triggereed.
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}
 
std::unique_ptr<Server> server;

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
  server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void HandleTerminate(int signal) {
  if (server) {
    std::cout << "Shutting down." << std::endl;
    server->Shutdown();
  }
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "Usage: gfs_master path_to_sqlite_database" << std::endl;
    return 1;
  }

  std::signal(SIGINT, HandleTerminate);
  std::signal(SIGTERM, HandleTerminate);
  RunServer(argv[1]);

  return 0;
}
