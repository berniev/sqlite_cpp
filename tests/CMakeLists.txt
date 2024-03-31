include(CTest)
include(GoogleTest)

add_executable(Tests
        cpp4sqlite_test.cpp
)
add_subdirectory(stuff)
add_dependencies(Tests
        assets
)
target_link_libraries(Tests PUBLIC
        GTest::gtest_main
        cpp4sqlite
)
gtest_discover_tests(Tests)
