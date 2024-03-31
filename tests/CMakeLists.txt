include(CTest)

add_executable(Tests
        cpp4sqlite_test.cpp
)
add_subdirectory(stuff)
add_dependencies(Tests
        assets
)
target_link_libraries(Tests PUBLIC
        gtest_main
        gmock_main
        cpp4sqlite
)
