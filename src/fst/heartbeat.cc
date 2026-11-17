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
      if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
        v1::RegisterNodeRequest registration;
        *registration.mutable_node() = node_;
        v1::RegisterNodeResponse accepted;
        grpc::ClientContext register_context;
        register_context.set_deadline(std::chrono::system_clock::now() +
                                      std::chrono::seconds(2));
        stub_->RegisterNode(&register_context, registration, &accepted);
      } else if (status.ok() && response.report_inventory()) {
        auto inventory = service_.Inventory();
        inventory.set_node_id(node_.node_id());
        inventory.set_endpoint(node_.endpoint());
        inventory.set_incarnation(response.incarnation());
        google::protobuf::Empty empty;
        grpc::ClientContext report_context;
        report_context.set_deadline(std::chrono::system_clock::now() +
                                    std::chrono::seconds(3));
        const auto reported = stub_->ReportChunkStatus(&report_context, inventory, &empty);
        if (!reported.ok()) {
          Log("inventory_failed", "node=" + node_.node_id() + " code=" +
                                      std::to_string(reported.error_code()));
        }
      } else if (!status.ok()) {
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
