#pragma once
//builtin
#include <vector>
//external
#include <sqlite3.h>
#include <Eigen/Dense>

//internal


#ifndef VECTORDATABASE_TYPES_H
#define VECTORDATABASE_TYPES_H


#endif //VECTORDATABASE_TYPES_H

typedef Eigen::RowVectorXf Vec;
typedef Eigen::VectorXf ColVec;
struct TableRow{
    int id;
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