#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <inttypes.h>

#include <grpc++/grpc++.h>
#include "gfs_common.h"
#include "gfs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::GFSBenchmark;
using gfs::AddTestInfoRequest;
using gfs::AddTestInfoReply;
using gfs::AddDataRequest;
using gfs::AddDataReply;

// Logic and data behind the server's behavior.
class GFSBenchmarkServerImpl final : public GFSBenchmark::Service {
 public:
  GFSBenchmarkServerImpl() {}
  ~GFSBenchmarkServerImpl() {}

  Status AddTestInfo(ServerContext* context, const AddTestInfoRequest* request,
   AddTestInfoReply* reply) override {
    info = request->info(); // in clock() num of cycles
    std::cout << "Got test info: \n" << info << '\n';
    reply->set_message("OK_AddTestInfo");
    return Status::OK;
  }
  
  Status AddData(ServerContext* context, const AddDataRequest* request,
   AddDataReply* reply) override {
    int duration = request->duration(); // in clock() num of cycles
    data.push_back(duration);
    std::cout << duration << '\n';
    reply->set_message("OK_AddData");
    return Status::OK;
  }

 private:
  std::string info;
  std::vector<int> data;
};

int main(int argc, char** argv) {
  std::string server_address("127.0.0.1:8888");
  GFSBenchmarkServerImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();

  return 0;
}
