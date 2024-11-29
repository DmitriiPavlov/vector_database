cmake_minimum_required(VERSION 3.28)
project(vectorDatabase)

set(CMAKE_CXX_STANDARD 17)

add_executable(vectorDatabase main.cpp "${PROJECT_SOURCE_DIR}/external/sqlite-amalgamation-3460100/sqlite3.c" "/Users/bison/Documents/Personal Projects/vectorDatabase/external/yyjson/yyjson.c"
        src/database.h
        src/types.h
        src/conversion.h
        src/locality_hashing.h
        src/bindings.h
        src/rng.h
        src/sqlwrapper.h
        src/client.h
)

include_directories("/Users/bison/Documents/Personal Projects/vectorDatabase/external/eigen-3.4.0")
include_directories("/Users/bison/Documents/Personal Projects/vectorDatabase/external/sqlite-amalgamation-3460100")
include_directories("/Users/bison/Documents/Personal Projects/vectorDatabase/external/yyjson")
