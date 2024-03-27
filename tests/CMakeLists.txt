add_executable(Tests
        cpp4sqlite_test.cpp
)
add_custom_target(assets
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR}/stuff ${CMAKE_BINARY_DIR}/stuff
)
add_dependencies(Tests
        assets
)
target_link_libraries(Tests PUBLIC
        gtest_main
        gmock_main
        cpp4sqlite
)
