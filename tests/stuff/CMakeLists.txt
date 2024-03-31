add_custom_target(assets
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR}
)
add_dependencies(Tests
        assets
)
