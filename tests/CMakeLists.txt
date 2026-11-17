find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(smoke_tests smoke_test.cc)
target_link_libraries(smoke_tests PRIVATE mgm_service GTest::gtest_main)
gtest_discover_tests(smoke_tests PROPERTIES TIMEOUT 10)
