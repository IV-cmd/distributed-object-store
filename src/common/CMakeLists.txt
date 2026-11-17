find_path(XXHASH_INCLUDE_DIR xxhash.h REQUIRED)
find_library(XXHASH_LIBRARY xxhash REQUIRED)

add_library(rs_codec rs_codec.cc)
target_include_directories(rs_codec PUBLIC "${PROJECT_SOURCE_DIR}/src")

add_library(objectstore_common chunk.cc)
target_include_directories(objectstore_common PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(objectstore_common SYSTEM PRIVATE "${XXHASH_INCLUDE_DIR}")
target_link_libraries(objectstore_common PUBLIC rs_codec "${XXHASH_LIBRARY}")
