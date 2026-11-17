#include "fst/heartbeat.h"

#include <exception>
#include <utility>

#include "common/log.h"

namespace objectstore {

HeartbeatSender::HeartbeatSender(FstService& service, v1::NodeInfo node,
                                   const std::shared_ptr<grpc::Channel>& channel,
                                   std::chrono::milliseconds interval)
    : service_(service),
      node_(std::move(node)),
      stub_(v1::MgmService::NewStub(channel)),
      interval_(interval),
      thread_([this](std::stop_token stop) { Run(stop); }) {}

void HeartbeatSender::Run(std::stop_token stop) {
  while (!stop.stop_requested()) {
    try {
      v1::HeartbeatRequest request;
      request.set_node_id(node_.node_id());
      request.set_endpoint(node_.endpoint());
      service_.FillHeartbeat(request);
      v1::HeartbeatResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(2));
      const auto status = stub_->Heartbeat(&context, request, &response);
      if (!status.ok()) {
        Log("heartbeat_failed", "node=" + node_.node_id() + " code=" +
                                    std::to_string(status.error_code()));
      }
    } catch (const std::exception& error) {
      Log("heartbeat_failed", "node=" + node_.node_id() + " error=" + error.what());
    }
    std::unique_lock lock(mutex_);
    wake_.wait_for(lock, stop, interval_, [] { return false; });
  }
}

}  // namespace objectstore
