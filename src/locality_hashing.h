#pragma once

//builtin

//external
#include "Eigen/Dense"
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
    uint16_t result = 0;
    for (int i = 0; i < 16; i++){
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

struct HeapComparator{
    bool operator()(const SortHelperStruct& a, const SortHelperStruct& b){
        return a.cosineIndex > b.cosineIndex;
    }
};

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

constexpr std::array<uint16_t, 16> createOpsArray(){
    std::array<uint16_t,16> out = std::array<uint16_t,16>();
    for (int i = 0; i < 16; i++){
        out[i] = 1<<i;
    }
    return out;
}

std::vector<uint16_t> generate_16CN_operations(const std::array<uint16_t,16>& ops,int n){
    std::vector<uint16_t> unfiltered_out;
    std::vector<uint16_t> newVec;
    unfiltered_out.push_back(0);
    for (int i = 0; i < n; i++){
        int start_size = unfiltered_out.size();
        for (uint16_t op : ops){
            for (int j = 0; j<start_size; j++){
                //we want to check whether any of the bits are already "turned on"
                //if we AND the bits together, and we get 0, that means there's no crossover, and we should add this brand-new operation
                if ((unfiltered_out[j]&op) == 0) {
                    newVec.push_back(unfiltered_out[j] ^ op);
                }
            }
        }
        unfiltered_out = newVec;
        newVec.clear();
    }
    std::vector<uint16_t> out;
    for (uint16_t op : unfiltered_out){
        auto index = std::find(out.begin(), out.end(), op);
        if (index == out.end()){
            out.push_back(op);
        }
    }
    return out;
}

long combi(int n,int k)
{
    long ans=1;
    k=k>n-k?n-k:k;
    int j=1;
    for(;j<=k;j++,n--)
    {
        if(n%j==0)
        {
            ans*=n/j;
        }else
        if(ans%j==0)
        {
            ans=ans/j*n;
        }else
        {
            ans=(ans*n)/j;
        }
    }
    return ans;
}



