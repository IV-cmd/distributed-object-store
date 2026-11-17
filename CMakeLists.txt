cmake_minimum_required(VERSION 3.24)
project(distributed_object_store VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(CTest)
add_compile_options(-Wall -Wextra -Wpedantic)

add_subdirectory(proto)
add_subdirectory(src/mgm)
add_subdirectory(src/fst)
add_subdirectory(src/client)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
