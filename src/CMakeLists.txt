add_library(cpp4sqlite SHARED
        cpp4sqlite.cpp
)
target_include_directories(cpp4sqlite PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)
find_package(SQLite3 REQUIRED)
target_link_libraries(cpp4sqlite PUBLIC
        SQLite::SQLite3
)
target_compile_features(cpp4sqlite PUBLIC cxx_std_17)