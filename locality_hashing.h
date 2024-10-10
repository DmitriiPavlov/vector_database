#pragma once

//builtin

//external
#include <Eigen/Dense>
//internal
#include "types.h"

#ifndef VECTORDATABASE_LOCALITY_HASHING_H
#define VECTORDATABASE_LOCALITY_HASHING_H

#endif

int hashPlane(const Vec& v, const Vec& random_plane_vector){
    return (v.dot(random_plane_vector)) >= 0;
}

Eigen::MatrixXf composeHashMatrix(std::vector<TableRow> data){
    Eigen::MatrixXf out(16,data[0].vector.size());
    for (int i = 0; i < data.size(); i++){
        out.row(i) = data[i].vector;
    }
    return out;
}

uint16_t hashVector(const ColVec& v,const Eigen::MatrixXf& hash_matrix){
    ColVec column = hash_matrix*v;
    int16_t result = 0;
    for (int i = 0; i < column.size(); i++){
        if (column[i]>=0){
            result+=1;
        }
        result = result<<1;
    }
    return result;
}

inline bool compareVecs(SortHelperStruct a, SortHelperStruct b){
    return a.cosineIndex < b.cosineIndex;
}

//sorthelperstruct doesn't need to have the cosine index initalized
void sortOnCosineSimilarity(std::vector<SortHelperStruct> * list, Vec queryVector){

    for (SortHelperStruct& e : *list){
        e.cosineIndex = e.row.vector.dot(queryVector);
    }

    std::sort(list->begin(),list->end(),compareVecs);
}

void printBinary(uint16_t num){
    std::bitset<16>a(num);
    std::cout<<a<<std::endl;
}



