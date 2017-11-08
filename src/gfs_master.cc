#include <csignal>

#include "gfs_master.h"

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
}

GFSMasterImpl::~GFSMasterImpl() {
  sqlite3_close(db_);
}

Status GFSMasterImpl::GetChunkhandle(ServerContext* context,
                                     const GetChunkhandleRequest* request,
                                     GetChunkhandleReply* reply) {
  const std::string& filename = request->filename();
  const int64_t chunk_index = request->chunk_index();

  // Try to insert file, ignoring if it already exists.
  sqlite3_stmt *insert_file_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "INSERT OR IGNORE INTO file (filename) VALUES (?)",
      -1, &insert_file_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_text(insert_file_stmt,
      1, filename.c_str(), filename.length(), SQLITE_STATIC));
  ThrowIfSqliteFailed(sqlite3_step(insert_file_stmt));
  ThrowIfSqliteFailed(sqlite3_finalize(insert_file_stmt));

  // Get file id.
  sqlite3_stmt *select_file_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "SELECT file_id FROM file WHERE filename=?",
      -1, &select_file_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_text(select_file_stmt,
      1, filename.c_str(), filename.length(), SQLITE_STATIC));
  ThrowIfSqliteFailed(sqlite3_step(select_file_stmt));
  int64_t file_id = sqlite3_column_int64(select_file_stmt, 0);
  ThrowIfSqliteFailed(sqlite3_finalize(select_file_stmt));

  // Try to insert chunk, ignoring if it already exists
  sqlite3_stmt *insert_chunk_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "INSERT OR IGNORE INTO chunk (file_id, chunk_index) VALUES (?, ?)",
      -1, &insert_chunk_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_int64(insert_chunk_stmt, 1, file_id));
  ThrowIfSqliteFailed(sqlite3_bind_int64(insert_chunk_stmt, 2, chunk_index));
  ThrowIfSqliteFailed(sqlite3_step(insert_chunk_stmt));
  ThrowIfSqliteFailed(sqlite3_finalize(insert_chunk_stmt));

  // Get chunkhandle.
  sqlite3_stmt *select_chunk_stmt;
  ThrowIfSqliteFailed(sqlite3_prepare_v2(db_,
      "SELECT chunkhandle FROM chunk WHERE file_id=? AND chunk_index=?",
      -1, &select_chunk_stmt, nullptr));
  ThrowIfSqliteFailed(sqlite3_bind_int64(select_chunk_stmt, 1, file_id));
  ThrowIfSqliteFailed(sqlite3_bind_int64(select_chunk_stmt, 2, chunk_index));
  ThrowIfSqliteFailed(sqlite3_step(select_chunk_stmt));
  int64_t chunkhandle = sqlite3_column_int64(select_chunk_stmt, 0);
  ThrowIfSqliteFailed(sqlite3_finalize(select_chunk_stmt));
  
  reply->set_chunkhandle(chunkhandle);
  return Status::OK;
}

Status GFSMasterImpl::ListFiles(ServerContext* context,
                                const ListFilesRequest* request,
                                ListFilesReply* reply) {
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

void GFSMasterImpl::ThrowIfSqliteFailed(int rc) {
  if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
    throw std::runtime_error(sqlite3_errmsg(db_));
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
