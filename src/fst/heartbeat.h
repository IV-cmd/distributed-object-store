#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "fst/service.h"

namespace objectstore {

class HeartbeatSender {
 public:
  HeartbeatSender(FstService& service, v1::NodeInfo node,
                  const std::shared_ptr<grpc::Channel>& channel,
                  std::chrono::milliseconds interval);

 private:
  void Run(std::stop_token stop);

  FstService& service_;
  v1::NodeInfo node_;
  std::unique_ptr<v1::MgmService::Stub> stub_;
  std::chrono::milliseconds interval_;
  std::mutex mutex_;
  std::condition_variable_any wake_;
  std::jthread thread_;
};

}  // namespace objectstore
