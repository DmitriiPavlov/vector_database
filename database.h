#pragma once
//builtin
#include <string>
#include <iostream>
#include <format>
//external
#include <sqlite3.h>
#include <Eigen/Dense>

//internal
#include "types.h"
#include "conversion.h"
#include "locality_hashing.h"

#ifndef VECTORDATABASE_QUERIES_H
#define VECTORDATABASE_QUERIES_H

#endif //VECTORDATABASE_QUERIES_H

//CALLBACKS

int printAll(void* data, int col_num, char** col_value, char** col_name){
    for (int i = 0; i < col_num; i++){
        std::cout<<col_name[i]<<" : "<<col_value[i];
    }
    return 0;
}

int writeOutput(void *data, int argc, char **argv, char **azColName) {
    // Cast the `data` pointer to a `std::string*` to store the results
    std::string *resultStr = static_cast<std::string*>(data);

    // Loop through each column in the current row
    for (int i = 0; i < argc; i++) {
        *resultStr += (argv[i] ? argv[i] : "NULL"); // Column value (check for NULL)
    }

    // Add a newline at the end of the row
    *resultStr += "\n";

    return 0; // Returning 0 allows sqlite3_exec to continue processing
}

int fetchVectorsCallback(void * data, int argc, char **argv, char **azColName){
    VectorQueryOutput *result_list = static_cast<VectorQueryOutput*>(data);
    TableRow row = {};
    for (int i = 0; i < argc; i++){
        if (i==0){
            row.id = std::atoi(argv[i]);
        }
        if (i==1){
            row.vector = convertToVec(argv[i],result_list->vector_size);
        }
        if (i==2){
            row.metadata = argv[i];
        }
    }
    result_list->rowList->push_back(row);

    return 0;
}

class Database {
public:
    sqlite3* db = nullptr;
    int _vector_size;
    int _random_vector_amount = 16;
    Eigen::MatrixXf hashMatrix;
    std::string sqlBatch;
    int average_match_index = 0;

    musqlite3_query insert_vector_query;
    musqlite3_query select_vector_query;

    musqlite3_query insert_rand_vector_query;
    musqlite3_query fetch_all_rand_vector_query;

    musqlite3_query select_metadata_query;
    musqlite3_query count_vector_query;
    //will create a Database if one doesn't exist
    explicit Database(const std::string& fileName, int vector_size = -1){
        _vector_size = vector_size;

        openDatabase(fileName);
        initSqlQueries();

        validateTables();
        ensureCorrectVectorSize(vector_size);
        loadHashMatrix();
    }

    ~Database(){
        int error_code = sqlite3_close(db);
        if (error_code != SQLITE_OK){
            std::cout<<sqlite3_errstr(error_code);
        }
    }

    void putVector(const Vec& vector, std::string metadata = ""){
        if (vector.size() != _vector_size){
            std::cout<<"Catostrophic error. Vector size not the same on insert.";
            return;
        }
        uint16_t key = hashVector(vector.transpose(),hashMatrix);
        Vec normalized_vector = vector/vector.norm();

        sqlite3_bind_int(insert_vector_query.get(),1,key);
        sqlite3_bind_blob(insert_vector_query.get(),2,normalized_vector.data(),sizeof(float)*_vector_size,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_vector_query.get(),3,metadata.c_str(),-1,SQLITE_TRANSIENT);
        sqlite3_step(insert_vector_query.get());
        sqlite3_reset(insert_vector_query.get());
    }

    void putRandVector(){
        Vec vector = Eigen::VectorXf::Random(_vector_size);
        vector = vector/vector.norm();
        uint16_t key = hashVector(vector.transpose(),hashMatrix);
        Vec normalized_vector = vector/vector.norm();

        sqlite3_bind_int(insert_vector_query.get(),1,key);
        sqlite3_bind_blob(insert_vector_query.get(),2,normalized_vector.data(),sizeof(float)*_vector_size,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_vector_query.get(),3,"",-1,SQLITE_TRANSIENT);
        sqlite3_step(insert_vector_query.get());
        sqlite3_reset(insert_vector_query.get());
    }

    int countVectors(int minKey, int maxKey){
        sqlite3_bind_int(count_vector_query.get(),1,minKey);
        sqlite3_bind_int(count_vector_query.get(),2,maxKey);
        sqlite3_step(count_vector_query.get());
        int out = sqlite3_column_int(count_vector_query.get(),0);
        return out;
    }

    int countVectors(std::string table){
        std::string sql = "SELECT COUNT(*) FROM "+table;
        std::unique_ptr<std::string> out = easyQuery(sql);
        return std::stoi((*out));
    }

    VectorQueryOutput fetchVectors(uint16_t minKey, uint16_t maxKey){
        VectorQueryOutput out = {_vector_size,std::make_unique<std::vector<TableRow>>()};

        sqlite3_bind_int(select_vector_query.get(),1,minKey);
        sqlite3_bind_int(select_vector_query.get(),2,maxKey);

        while(sqlite3_step(select_vector_query.get()) != SQLITE_DONE){
            int id = sqlite3_column_int(select_vector_query.get(),0);
            Vec v = convertToVecFromBLOB(sqlite3_column_blob(select_vector_query.get(),1),_vector_size);
            std::string meta = "";
            if (select_vector_query.get() != nullptr) {
                meta = std::string(reinterpret_cast<const char *>(sqlite3_column_text(select_vector_query.get(), 2)));
            }
            TableRow new_row = {id,v,};
            out.rowList->push_back(new_row);
        }
        return out;
    }

    VectorQueryOutput fetchRandomVectors(){
        std::string sql = "SELECT * FROM random_vectors";
        VectorQueryOutput output = {_vector_size,std::make_unique<std::vector<TableRow>>()};
        easyQuery(sql,fetchVectorsCallback,&output);
        return output;
    }

    Vec fetchClosestVector(const Vec& queryVector){
        uint16_t query_hash = hashVector(queryVector,hashMatrix);
        //all 1's in binary
        uint16_t largest_int16 = 65535;
        VectorQueryOutput output;
        for (int i = 0; i < 16; i++){
            uint16_t current_query_hash_min = ((query_hash>>i)<<i);
            uint16_t current_query_hash_max = current_query_hash_min + (largest_int16>>(16-i));

            if (countVectors(current_query_hash_min,current_query_hash_max) > 0){
                average_match_index += i;
                std::cout<<"here";
                output = fetchVectors(current_query_hash_min,current_query_hash_max);
                break;
            }

        }

        std::vector<SortHelperStruct> toGetSorted;
        for (TableRow r : *output.rowList){
            SortHelperStruct e = {0,r};
            toGetSorted.push_back(e);
        }
        sortOnCosineSimilarity(&toGetSorted,queryVector);

        return toGetSorted[toGetSorted.size()-1].row.vector;
    }

    //this should be done with a queue
    std::vector<TableRow> fetchClosestVectors(const Vec& queryVector, int quantity){
        uint16_t query_hash = hashVector(queryVector,hashMatrix);
        //all 1's in binary
        uint16_t largest_int16 = 65535;
        VectorQueryOutput output;
        for (int i = 0; i < 16; i++){
            uint16_t current_query_hash_min = ((query_hash>>i)<<i);
            uint16_t current_query_hash_max = current_query_hash_min + (largest_int16>>(16-i));

            if (countVectors(current_query_hash_min,current_query_hash_max) >= quantity){
                average_match_index += i;
                output = fetchVectors(current_query_hash_min,current_query_hash_max);
                break;
            }

        }

        std::vector<SortHelperStruct> toGetSorted;
        for (TableRow r : *output.rowList){
            SortHelperStruct e = {0,r};
            toGetSorted.push_back(e);
        }
        sortOnCosineSimilarity(&toGetSorted,queryVector);

        std::vector<TableRow> return_list;

        for (int i = 0; i < quantity; i++){
            return_list.push_back(toGetSorted[toGetSorted.size()-i-1].row);
        }



        return return_list;
    }



private:
    void loadHashMatrix(){
        VectorQueryOutput vecs = fetchRandomVectors();
        hashMatrix = composeHashMatrix(*vecs.rowList);
    }

    void openDatabase(const std::string & fileName){
        int error_code = sqlite3_open_v2(fileName.c_str(), &db, SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE, NULL);
        if (error_code != SQLITE_OK){
            std::cout<<"Failed to open:"<<std::endl;
            std::cout<<sqlite3_errstr(error_code);
        }
    }
    void validateTables() {
        std::string create_metadata_table_query = "CREATE TABLE IF NOT EXISTS metadata (\n"
                                                  "    key TEXT NOT NULL,\n"
                                                  "    value INT NOT NULL\n"
                                                  ");";
        easyQuery(create_metadata_table_query,NULL,NULL);

        std::string create_vectors_table_query = "CREATE TABLE IF NOT EXISTS vectors (\n"
                                                  "    key INT NOT NULL,\n"
                                                  "    vector BLOB NOT NULL,\n"
                                                 "     metadata TEXT\n"
                                                  ");";
        easyQuery(create_vectors_table_query,NULL,NULL);
        easyQuery("CREATE INDEX IF NOT EXISTS idx_hash ON vectors(key);",NULL,NULL);

        std::string create_random_vectors_table_query = "CREATE TABLE IF NOT EXISTS random_vectors (\n"
                                                 "    key INT NOT NULL,\n"
                                                 "    vector TEXT NOT NULL,\n"
                                                 "     metadata TEXT\n"
                                                 ");";
        easyQuery(create_random_vectors_table_query,NULL,NULL);

        validateRandomVectorsTable();
    }

    void validateRandomVectorsTable(){
        if (countVectors("random_vectors")!= _random_vector_amount){
            std::cout<<countVectors("random_vectors");
            for (int i = 0; i < _random_vector_amount; i++) {
                putRandVector();
            }
        }
    }

    void ensureCorrectVectorSize(int vector_size){
        std::unique_ptr<std::string> vector_size_str = easyQuery("SELECT DISTINCT value FROM metadata WHERE key='vector_size'");

        if ((*vector_size_str).empty()){
            setVectorSize();
        }
        else {
            _vector_size = std::stoi(*vector_size_str);
            if (vector_size != _vector_size) {
                std::cout << "Size of vector not the same as declared in database";
            }
        }
    }

    void easyQuery(std::string sql, int (*callback)(void*,int,char**,char**),void *data){
        int error_code = sqlite3_exec(db,sql.c_str(),callback,data,NULL);
        if (error_code != SQLITE_OK){
            std::cout<<sqlite3_errstr(error_code)<<"from : "<<sql;
        }
    }

    std::unique_ptr<std::string> easyQuery(std::string sql){
        std::unique_ptr<std::string> output = std::make_unique<std::string>("");

        int error_code = sqlite3_exec(db,sql.c_str(),writeOutput,output.get(),NULL);
        if (error_code != SQLITE_OK){
            std::cout<<sqlite3_errstr(error_code)<<"from : "<<sql;
        }
        return output;
    }

    void insertMetadata(std::string key, int value){
        std::string sql =
                "INSERT INTO metadata(key,value) VALUES('"+key+"'," + std::to_string(value) + ");";
        easyQuery(sql,NULL,NULL);
    }

    void setVectorSize(){
        if(_vector_size != -1) {
            insertMetadata("vector_size",_vector_size);
        }
        else{
            std::cout<<"ERROR: Vector size was not provided";
        }
    }

    void initSqlQueries(){
        initSqlQuery("INSERT INTO vectors(key,vector,metadata) VALUES(?,?,?)",insert_vector_query);
        initSqlQuery("INSERT INTO random_vectors(key,vector,metadata) VALUES(?,?,?)",insert_rand_vector_query);
        initSqlQuery("SELECT * FROM vectors WHERE key>=? AND key<=?",select_vector_query);
        initSqlQuery("SELECT * FROM rand_vectors",fetch_all_rand_vector_query);
        initSqlQuery("INSERT INTO vectors(key,vector,metadata) VALUES(?,?,?)",insert_vector_query);
        initSqlQuery("SELECT COUNT(*) FROM vectors WHERE key>=? AND key<=?",count_vector_query);
    }

    void initSqlQuery(const char * sql, musqlite3_query& query){
        sqlite3_stmt * raw_query;
        sqlite3_prepare_v2(db,sql,-1,&raw_query,nullptr);
        query = musqlite3_query(raw_query);
    }
};
