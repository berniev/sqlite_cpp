cmake_minimum_required(VERSION 3.16.3 FATAL_ERROR)

project(cpp4sqlite,
        VERSION 0.1
        DESCRIPTION "cpp front end for sqlite"
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SQLite3)

add_subdirectory(src)
add_subdirectory(ext/googletest)
add_subdirectory(tests)
