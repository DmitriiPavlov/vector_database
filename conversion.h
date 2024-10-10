#pragma once
//builtin
#include <string>
//external

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