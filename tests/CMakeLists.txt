find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(smoke_tests smoke_test.cc)
target_link_libraries(smoke_tests PRIVATE mgm_service GTest::gtest_main)
gtest_discover_tests(smoke_tests PROPERTIES TIMEOUT 10)

add_executable(chunk_tests chunk_test.cc)
target_link_libraries(chunk_tests PRIVATE objectstore_common GTest::gtest_main)
gtest_discover_tests(chunk_tests PROPERTIES TIMEOUT 10)
