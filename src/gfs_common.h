#include <grpc++/grpc++.h>
#include <string>

#include "gfs.grpc.pb.h"

using gfs::CopyChunksReply;
using gfs::CopyChunksRequest;
using gfs::DeleteChunksReply;
using gfs::DeleteChunksRequest;
using gfs::DeleteFileReply;
using gfs::DeleteFileRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindLeaseHolderRequest;
using gfs::FindLocationsReply;
using gfs::FindLocationsRequest;
using gfs::FindMatchingFilesReply;
using gfs::FindMatchingFilesRequest;
using gfs::GFS;
using gfs::GFSMaster;
using gfs::GetFileLengthReply;
using gfs::GetFileLengthRequest;
using gfs::HeartbeatReply;
using gfs::HeartbeatReply;
using gfs::HeartbeatRequest;
using gfs::HeartbeatRequest;
using gfs::MoveFileReply;
using gfs::MoveFileRequest;
using gfs::PingReply;
using gfs::PingRequest;
using gfs::PushDataReply;
using gfs::PushDataRequest;
using gfs::ReadChunkReply;
using gfs::ReadChunkRequest;
using gfs::ReplicateChunksReply;
using gfs::ReplicateChunksReply;
using gfs::ReplicateChunksRequest;
using gfs::ReplicateChunksRequest;
using gfs::SerializedWriteReply;
using gfs::SerializedWriteRequest;
using gfs::WriteChunkReply;
using gfs::WriteChunkRequest;
using google::protobuf::Timestamp;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

#define CHUNK_SIZE_IN_BYTES (64 * (1 << 20)) // 64MiB
#define NUM_CHUNKSERVER_REPLICAS 3
#define MAX_CLIENT_RETRIES 5

// Time before chunkserver lease expires at the master.
#define LEASE_DURATION_SECONDS 10

// Duration between heartbeats at chunkserver.
#define HEARTBEAT_DURATION_SECONDS 2

std::string FormatStatus(const Status& status);
