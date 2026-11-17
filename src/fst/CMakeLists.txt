add_library(fst_service service.cc heartbeat.cc)
target_include_directories(fst_service PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(fst_service PUBLIC objectstore_transfer)

add_executable(fst main.cc)
target_link_libraries(fst PRIVATE fst_service)
