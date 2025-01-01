//external

//builtin
#include <chrono>

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

    //search parameters, can be used if we want auto-optimization
    float internal_min_threshold = -1.0f;
    //in seconds
    float desired_latency;
    bool optimizing = false;

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

    void insertVector(const std::string& json_data){
        auto pair = convertToInputFromJson(json_data,wrapper._vector_size);
        wrapper.insert(pair.vector,pair.metadata);
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

    std::vector<std::pair<TableRow,float>> fetchNVectors(const Vec& v,int n, float minthreshold, bool linearSearch){
        syncBuffer();
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
        //we can run a linear search just to make sure its ok
        if (linearSearch){
            linearSearchHelper(output,v);
        }
        return output;
    }


    std::vector<std::pair<TableRow,float>> fetchNVectorsAutoOptimized(const Vec& v, int n,bool linearsearch){
        if (!optimizing){
            return fetchNVectors(v,n,internal_min_threshold,false);
        }
        else {
            double derivative = computeDerivativeHelper(v,n,linearsearch);
        }
    }


    void setDesiredLatency(float latency){
        desired_latency = latency;
        optimizing = true;
    }

    std::string fetchNVectorsJSON(const Vec& v, int n, float minthreshold, bool linearSearch){
        return convertToJsonFromOutput(fetchNVectors(v,n,minthreshold,linearSearch));
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

    //this should iterate through all the vectors in the database, and only get called for really tiny vector database sizes
    //the basic idea is that if the user wants to search the database, and the amount of vectors that the algorithm has to look through
    //is comparable to the total amount of the vectors in the database, then this should be invoked
    void linearSearchHelper(std::vector<std::pair<TableRow,float>> & output,Vec v){
        wrapper.beginAllSelect();
        TableRow curr_row;
        while (true){
            curr_row = wrapper.stepAllSelect();
            if (!curr_row.valid){
                break;
            }
            else{
                insertRow(std::pair<TableRow, float>(curr_row,curr_row.vector.dot(v)),output);
            };
        }
    }

    double computeDerivativeHelper(const Vec&v, int n, bool linearsearch){
        std::vector<std::pair<TableRow,float>> result;
        auto start = std::chrono::high_resolution_clock::now();
        result = fetchNVectors(v,n,internal_min_threshold-0.01f,linearsearch);
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double latency_a = duration.count();


        start = std::chrono::high_resolution_clock::now();
        result = fetchNVectors(v,n,internal_min_threshold+ 0.01f,linearsearch);
        end = std::chrono::high_resolution_clock::now();
        duration = end - start;
        double latency_b = duration.count();

        return (latency_b - latency_a)/(0.02f);
    }

};


#endif
