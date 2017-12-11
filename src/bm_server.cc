#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <inttypes.h>
#include <time.h>
#include <mutex>

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
    bm_mutex.lock();
    int id = request->id();
    operation[id] = request->operation();
    method[id] = request->method();
    size[id] = request->size();
    std::cout << "Got info for Client[" << id << "] = "
              << operation[id] << ", "
              << method[id] << ", "
              << size[id] << std::endl;
    reply->set_message("OK_ClientBMHandshake");
    bm_mutex.unlock();
    return Status::OK;
  }
  
  Status AddData(ServerContext* context, const AddDataRequest* request,
   AddDataReply* reply) override {
    bm_mutex.lock();
    int id = request->id();
    int duration = request->duration(); // in nanosecond
    clock_gettime(CLOCK_REALTIME, &now);
    if (time_data[id].size() >= 1 && now.tv_sec != time_data[id].back().tv_sec) {
      PrintLastSecAverage(id);
    }
    
    duration_data[id].push_back(duration);
    throughput_data[id].push_back((double) size[id]/(duration*1e-9));
    time_data[id].push_back(now);
    reply->set_message("OK_AddData");
    bm_mutex.unlock();
    return Status::OK;
  }

 private:
  void PrintLastSecAverage(int id) {
    for (auto i = throughput_data.begin(); i != throughput_data.end(); ++i) {
      avg_throughput_data[i->first] = 0;
    }
    // Calculate avg of all client
    for (auto i = avg_throughput_data.begin(); i != avg_throughput_data.end(); ++i) {
      int id = i->first, size = throughput_data[id].size();
      while (!throughput_data[id].empty()) {
        i->second += (double)throughput_data[id].back() / size;
        throughput_data[id].pop_back();
        time_data[id].pop_back();
        duration_data[id].pop_back();
      }
    }

    double total_throughput = 0;
    for (auto i = avg_throughput_data.begin(); i != avg_throughput_data.end(); ++i) {
      total_throughput += i->second;
      std::cout << "Avg for Client[" << i->first << "] throughput (B/s) = "
                << avg_throughput_data[i->first] << std::endl;
    }

    if (avg_throughput_data.size() > 1) {
      std::cout << "Total throughput = " << total_throughput << std::endl;
    }
  }

  timespec now;
  std::mutex bm_mutex;

  std::map<int, std::string> operation;
  std::map<int, std::string> method;
  std::map<int, int> size;
  std::map<int, std::vector<long long>> duration_data;
  std::map<int, std::vector<double>> throughput_data;
  std::map<int, double> avg_throughput_data; 
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
