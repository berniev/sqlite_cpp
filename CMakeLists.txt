cmake_minimum_required(VERSION 3.16.3 FATAL_ERROR)

project(cpp4sqlite,
        VERSION 0.1
        DESCRIPTION "cpp front end for sqlite"
        LANGUAGES CXX
)

add_subdirectory(src)
add_subdirectory(ext)
add_subdirectory(tests)
