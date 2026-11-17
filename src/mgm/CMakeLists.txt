add_library(mgm_service service.cc placement.cc)
target_include_directories(mgm_service PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(mgm_service PUBLIC objectstore_proto objectstore_common)

add_executable(mgm main.cc)
target_link_libraries(mgm PRIVATE mgm_service)
