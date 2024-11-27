//builtin
#include <random>
//external
//internal
#include "types.h"
#ifndef VECTORDATABASE_RNG_H
#define VECTORDATABASE_RNG_H

#include <random>
#include <iostream>

Vec genRandVec(int size) {
    Vec out(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-1.0, 1.0); // Range: 0.0 to 1.0

    for (int i = 0; i < size; i++){
        double random_float = dis(gen);
        out[i] = random_float;
    }

    return out.normalized();
}

#endif //VECTORDATABASE_RNG_H
