#pragma once

#include <filesystem>
#include <memory>
#include <string>

#include "objectstore.grpc.pb.h"

namespace objectstore {

class ObjectClient {
 public:
  explicit ObjectClient(const std::shared_ptr<grpc::Channel>& channel);

  grpc::Status Put(const std::filesystem::path& file,
                   const std::string& object_id);
  grpc::Status Get(const std::string& object_id,
                   const std::filesystem::path& file);
  grpc::Status Remove(const std::string& object_id);

 private:
  std::unique_ptr<v1::FstService::Stub> stub_;
};

class ReplicatedClient {
 public:
  explicit ReplicatedClient(const std::shared_ptr<grpc::Channel>& channel);

  grpc::Status Put(const std::filesystem::path& file,
                   const std::string& object_id,
                   v1::Layout layout = v1::REPLICA3);
  grpc::Status Get(const std::string& object_id,
                   const std::filesystem::path& file);
  grpc::Status Remove(const std::string& object_id);
  grpc::Status Stat(const std::string& object_id, v1::FileMetadata& metadata);
  grpc::Status List(v1::FileList& files);
  grpc::Status Nodes(v1::NodeList& nodes);

 private:
  std::unique_ptr<v1::MgmService::Stub> stub_;
};

}  // namespace objectstore
