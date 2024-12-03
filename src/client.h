//external

//builtin

//internal
#include "sqlwrapper.h"
#include "locality_hashing.h"

#ifndef VECTORDATABASE_CLIENT_H
#define VECTORDATABASE_CLIENT_H

class DatabaseClient{
    InternalSQLWrapper wrapper;
    std::vector<Eigen::MatrixXf> hashMatrices;


    //search tomfoolery
    const std::array<uint16_t,16> ops = createOpsArray();
    std::array<std::vector<uint16_t>,4> op_variations;
    std::array<uint16_t,697> total_ops;

    //writing system
    std::pair<Vec,std::string> buffer[500];
    int index = 0;

public:
    //statistics variables
    int total_vector_amount = -1;

    //holy cursed logic that is necessary cause InternalSQLWrapper is immovable
    //potential fix is having the checks for whether file exists happen inside, but i dont like that solution
    explicit DatabaseClient(const std::string& filename, int vector_size,int key_count)
            : wrapper([&]() -> InternalSQLWrapper {
        if (!InternalSQLWrapper::dbExists(filename)) {
            InternalSQLWrapper::init(filename, vector_size, 16,key_count);
        }
        return InternalSQLWrapper(filename, vector_size, 16,key_count);
    }())
    {
        std::vector<Vec> all = wrapper.getAllRandomVectors();
        for (int i = 0; i < key_count; i++) {
            std::vector<Vec> temp;
            for (int j = 0; j < 16; j++) {
                temp.push_back(all[i * 16 + j]);
            }
            hashMatrices.push_back(composeHashMatrix(temp,wrapper._vector_size,wrapper._random_vector_amount));
        }

        total_vector_amount = wrapper.getTotalCount();

        for (int i = 0; i < op_variations.size(); i++){
            op_variations[i] = generate_16CN_operations(ops,i);
        }
        int index = 0;
        for (const auto& variation : op_variations){
            for (uint16_t op: variation){
                total_ops[index] = op;
                index++;
            }
        }
    }

    void insertVector(const Vec& v, const std::string& metadata){
        //writing buffer
        buffer[index] = std::pair<Vec,std::string>(v,metadata);
        index++;
        if (index == 500) {
            syncBuffer();
        }
        total_vector_amount++;
    }

    void syncBuffer(){
        std::vector<int> keys(wrapper._key_count);
        for (int i = 0; i < index; i++){
            auto& pair = buffer[i];
            for (int j = 0; j < keys.size(); j++){
                keys[j] = hashVector(pair.first,hashMatrices[j]);
            }
            wrapper.insert(keys,pair.first,pair.second);
        }
        index = 0;
    }




    std::vector<std::pair<TableRow,float>> fetchNVectors(const Vec& v,int n, float minthreshold){
        //sorted array
        std::vector<std::pair<TableRow,float>> output(n);
        Vec normalized_vector = v/v.norm();
        std::vector<uint16_t> keys(wrapper._key_count);
        int lazy_loaded = -1;

        for (const uint16_t& op: total_ops) {
            for (int i = 0; i < keys.size(); i++) {
                //implements lazy loading for the keys in the vectors, so that not all the keys have to be computed,
                //unless they are all necessary
                if (i > lazy_loaded){
                    keys[i] = hashVector(v,hashMatrices[i]);
                    lazy_loaded++;
                }

                wrapper.beginSelect(keys[i] ^ op, i);
                while (true) {
                    TableRow row = wrapper.stepSelect( i);
                    if (!row.valid) {
                        break;
                    }
                    float dot = normalized_vector.dot(row.vector);
                    insertRow(std::pair(row, dot), output);

                }
                if (output[n - 1].first.valid && output[n-1].second > minthreshold) {
                    wrapper.finishSelect(i);
                    return output;
                }
            }
        }

        return output;
    }

    std::vector<std::pair<TableRow,float>> fetchNVectors_v2(const Vec& v,int n, float minthreshold){
        //sorted array
        std::vector<std::pair<TableRow,float>> output(n);
        Vec normalized_vector = v/v.norm();
        std::vector<uint16_t> keys(wrapper._key_count);
        for (int i = 0; i < wrapper._key_count; i++){
            keys[i] = hashVector(v,hashMatrices[i]);
        }

        for (const uint16_t& op: total_ops) {
            for (int i = 0; i < keys.size(); i++) {
                wrapper.beginSelect(keys[i] ^ op, i);
                while (true) {
                    TableRow row = wrapper.stepSelect( i);
                    if (!row.valid) {
                        break;
                    }
                    float dot = normalized_vector.dot(row.vector);
                    if (dot > minthreshold){
                        insertRow(std::pair(row, dot), output);
                    }
                }
                if (output[n - 1].first.valid && output[n-1].second > minthreshold) {
                    wrapper.finishSelect(i);
                    return output;
                }
            }
        }

        return output;
    }
    ~DatabaseClient(){
        syncBuffer();
    }
private:
    void insertRow(std::pair<TableRow,float> temp_pair,std::vector<std::pair<TableRow,float>>& sorted_list){
        for (int i = 0; i < sorted_list.size(); i++) {
            if (!sorted_list[i].first.valid) {
                sorted_list[i] = temp_pair;
                break;
            }
            if (sorted_list[i].second < temp_pair.second) {
                std::swap(temp_pair, sorted_list[i]);
            }
        }
    }
};


#endif
