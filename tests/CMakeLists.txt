find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(smoke_tests smoke_test.cc)
target_link_libraries(smoke_tests PRIVATE mgm_service GTest::gtest_main)
gtest_discover_tests(smoke_tests PROPERTIES TIMEOUT 10)

add_executable(chunk_tests chunk_test.cc)
target_link_libraries(chunk_tests PRIVATE objectstore_common GTest::gtest_main)
gtest_discover_tests(chunk_tests PROPERTIES TIMEOUT 10)

add_executable(storage_tests storage_test.cc)
target_link_libraries(storage_tests PRIVATE fst_service objectstore_client GTest::gtest_main)
gtest_discover_tests(storage_tests PROPERTIES TIMEOUT 30)

add_executable(metadata_tests metadata_test.cc placement_test.cc recovery_test.cc)
target_link_libraries(metadata_tests PRIVATE mgm_service GTest::gtest_main)
gtest_discover_tests(metadata_tests PROPERTIES TIMEOUT 10)

add_executable(replication_tests replication_test.cc)
target_link_libraries(replication_tests PRIVATE mgm_service fst_service objectstore_client GTest::gtest_main)
gtest_discover_tests(replication_tests PROPERTIES TIMEOUT 30)

add_executable(repair_tests repair_test.cc)
target_link_libraries(repair_tests PRIVATE mgm_service fst_service objectstore_client GTest::gtest_main)
gtest_discover_tests(repair_tests PROPERTIES TIMEOUT 30)
