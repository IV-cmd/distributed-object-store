#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <exception>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "common/log.h"
#include "common/settings.h"
#include "mgm/service.h"

int main(int argc, char* argv[]) {
  if (argc > 2) {
    std::cerr << "Usage: mgm [listen_address]\n";
    return 2;
  }
  try {
    const std::string address = argc == 2 ? argv[1] : "127.0.0.1:50051";
    objectstore::MgmService service(
        objectstore::EnvMilliseconds("HEARTBEAT_TIMEOUT_MS", 6000));
    const auto interval = objectstore::EnvMilliseconds("REPAIR_INTERVAL_MS", 500);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) {
      std::cerr << "Failed to start MGM on " << address << '\n';
      return 1;
    }
    std::mutex mutex;
    std::condition_variable_any wake;
    std::jthread maintenance([&](std::stop_token stop) {
      while (!stop.stop_requested()) {
        try {
          service.Maintain();
        } catch (const std::exception& error) {
          objectstore::Log("maintenance_failed", error.what());
        }
        std::unique_lock lock(mutex);
        wake.wait_for(lock, stop, interval, [] { return false; });
      }
    });
    std::cout << "MGM listening on " << address << '\n' << std::flush;
    server->Wait();
  } catch (const std::exception& error) {
    std::cerr << "MGM: " << error.what() << '\n';
    return 1;
  }
}
