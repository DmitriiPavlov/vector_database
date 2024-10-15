#pragma once
//builtin
#include <string>
//external
#include <Eigen/Dense>
//internal
#include "types.h"

#ifndef VECTORDATABASE_CONVERSION_H
#define VECTORDATABASE_CONVERSION_H

#endif //VECTORDATABASE_CONVERSION_H


std::string convertToString(Vec vector){
    std::stringstream ss;
    for (int i = 0; i < vector.size(); i++){
        ss<<vector[i];
        if (i!=vector.size()-1){
            ss<<" ";
        }
    }
    return ss.str();
}

Vec convertToVec(std::string str, int size){
    std::istringstream iss(str);
    Vec out(size);
    float num;
    for (int i = 0; i < size; i++){
        iss>>num;
        out[i] = num;
    }
    return out;
}

Vec convertToVecFromBLOB(const void * blobPointer, int vec_size){
    Vec outVec(vec_size);
    std::memcpy(outVec.data(),blobPointer,sizeof(float)*vec_size);
    return outVec;
}

