#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <inttypes.h>
#include <time.h>

#include <grpc++/grpc++.h>
#include "gfs_common.h"
#include "gfs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::GFSBenchmark;
using gfs::ClientBMHandshakeRequest;
using gfs::ClientBMHandshakeReply;
using gfs::AddDataRequest;
using gfs::AddDataReply;

// Logic and data behind the server's behavior.
class GFSBenchmarkServerImpl final : public GFSBenchmark::Service {
 public:
  GFSBenchmarkServerImpl() {}
  ~GFSBenchmarkServerImpl() {}

  Status ClientBMHandshake(ServerContext* context, const ClientBMHandshakeRequest* request,
   ClientBMHandshakeReply* reply) override {
    int id = request->id();
    operation[id] = request->operation();
    method[id] = request->method();
    size[id] = request->size();
    std::cout << "Got info for Client[" << id << "] = "
              << operation[id] << ", "
              << method[id] << ", "
              << size[id] << std::endl;
    reply->set_message("OK_ClientBMHandshake");
    return Status::OK;
  }
  
  Status AddData(ServerContext* context, const AddDataRequest* request,
   AddDataReply* reply) override {
    int id = request->id();
    int duration = request->duration(); // in nanosecond
    clock_gettime(CLOCK_REALTIME, &now);
    if (time_data[id].size() >= 2 && now.tv_sec != time_data[id].back().tv_sec) {
      PrintLastSecAverage(id);
    }
    
    duration_data[id].push_back(duration);
    throughput_data[id].push_back((double) size[id]/(duration*1e-9));
    time_data[id].push_back(now);

    reply->set_message("OK_AddData");
    return Status::OK;
  }

  void PrintLastSecAverage(int id) {
    int sec = time_data[id].back().tv_sec;
    int i = time_data[id].size() - 1;
    while (i >= 0 && time_data[id][i].tv_sec == sec) {
      i--;
    }
    i++;

    long long num = time_data[id].size() - i;
    double duration_avg = 0.0;
    double throughput_avg = 0.0;
    for (; i < (int)time_data[id].size(); i++) {
      duration_avg += (duration_data[id][i] / num);
      throughput_avg += (throughput_data[id][i] / num);
    }

    std::cout << "Avg for Client[" << id << "] = duration (ns) =" << duration_avg
                << "   throughput (B/s) = " << throughput_avg << std::endl;
  }

 private:
  timespec now;
  std::map<int, std::string> operation;
  std::map<int, std::string> method;
  std::map<int, int> size;
  std::map<int, std::vector<int>> duration_data;
  std::map<int, std::vector<double>> throughput_data;
  std::map<int, std::vector<timespec>> time_data;
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
