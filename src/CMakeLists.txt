add_library(cpp4sqlite SHARED
        cpp4sqlite.cpp
)
target_include_directories(cpp4sqlite PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)
target_link_libraries(cpp4sqlite PUBLIC
        SQLite::SQLite3
)
