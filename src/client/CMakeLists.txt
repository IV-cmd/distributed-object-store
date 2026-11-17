add_library(objectstore_client client.cc replicated.cc erasure.cc)
target_include_directories(objectstore_client PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(objectstore_client PUBLIC objectstore_transfer)

add_executable(client main.cc)
target_link_libraries(client PRIVATE objectstore_client)
