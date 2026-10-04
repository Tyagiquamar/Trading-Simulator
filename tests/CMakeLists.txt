include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(test_order_book test_order_book.cpp)
target_link_libraries(test_order_book PRIVATE orderbook gtest_main)
target_include_directories(test_order_book PRIVATE ${CMAKE_SOURCE_DIR}/include)

find_package(Threads REQUIRED)

add_executable(test_net_framing test_net_framing.cpp)
target_link_libraries(test_net_framing PRIVATE gtest_main Threads::Threads)
target_include_directories(test_net_framing PRIVATE ${CMAKE_SOURCE_DIR}/include)

include(GoogleTest)
gtest_discover_tests(test_order_book)
gtest_discover_tests(test_net_framing)