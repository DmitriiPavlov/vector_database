#pragma once
//builtin
#include <vector>
//external
#include "sqlite3.h"
#include "Eigen/Dense"

//internal


#ifndef VECTORDATABASE_TYPES_H
#define VECTORDATABASE_TYPES_H


#endif //VECTORDATABASE_TYPES_H

typedef Eigen::RowVectorXf Vec;
typedef Eigen::VectorXf ColVec;
struct TableRow{
    bool valid = false;
    Vec vector;
    std::string metadata;
};
typedef struct TableRow TableRow;

struct VectorQueryOutput{
    int vector_size;
    std::unique_ptr<std::vector<TableRow>>rowList;
};
typedef struct VectorQueryOutput VectorQueryOutput;

struct SortHelperStruct{
    float cosineIndex;
    TableRow row;
};

typedef struct SortHelperStruct SortHelperStruct ;

struct SQLiteStmtDeleter {
    void operator()(sqlite3_stmt* stmt) const {
        sqlite3_finalize(stmt);
    }
};

typedef std::unique_ptr<sqlite3_stmt, SQLiteStmtDeleter> musqlite3_query;

struct musqlite3_batched_query_wrraper{
    musqlite3_query & wrapped_query;
    int index = 0;
    void bind(uint16_t key){
        index++;
        sqlite3_bind_int(wrapped_query.get(),index,key);
    }
};

struct fetch_query_output{
    bool success;
    std::vector<TableRow> result;
};

struct insert_query_input{
    std::string metadata;
    Vec vector;
};
typedef struct insert_query_input insert_query_input;