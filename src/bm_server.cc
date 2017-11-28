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
using gfs::AddConcurrentWriteClDataRequest;
using gfs::AddConcurrentWriteClDataReply;

// Logic and data behind the server's behavior.
class GFSBenchmarkServerImpl final : public GFSBenchmark::Service {
 public:
  GFSBenchmarkServerImpl() {}
  ~GFSBenchmarkServerImpl() {}
  
  Status AddConcurrentWriteClData(
    ServerContext* context,
    const AddConcurrentWriteClDataRequest* request,
    AddConcurrentWriteClDataReply* reply) override {
    int client_num = request->client_number();
    int duration_ms = request->duration_ms();
    concurrent_write_clientsN_time[client_num].push_back(duration_ms);

    reply->set_message("OK_AddConcurrentWriteClData");
    return Status::OK;
  }

  void PrintResult() {
    int median_index = concurrent_write_clientsN_time[1].size() / 2;
    std::cout << "READ: client_number[1] = " <<  concurrent_write_clientsN_time[1][median_index];
  }
 
 private:
  std::map<int, std::vector<int>> concurrent_write_clientsN_time;
};

int main(int argc, char** argv) {
  std::string server_address("127.0.0.1:88888");
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

  service.PrintResult();

  return 0;
}
