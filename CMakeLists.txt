cmake_minimum_required(VERSION 3.28)
project(vectorDatabase)

set(CMAKE_CXX_STANDARD 17)

add_executable(vectorDatabase main.cpp "${PROJECT_SOURCE_DIR}/external/sqlite-amalgamation-3460100/sqlite3.c"
        database.h
        types.h
        conversion.h
        locality_hashing.h
        queries.h)

include_directories("/Users/bison/Documents/Personal Projects/vectorDatabase/external/eigen-3.4.0")
include_directories("/Users/bison/Documents/Personal Projects/vectorDatabase/external/sqlite-amalgamation-3460100")
