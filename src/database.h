#pragma once
//builtin
#include <string>
#include <iostream>
#include <format>
//external
#include "sqlite3.h"
#include "Eigen/Dense"

//internal
#include "types.h"
#include "conversion.h"
#include "locality_hashing.h"
#include "rng.h"
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
    auto *resultStr = static_cast<std::string*>(data);

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

    const std::array<uint16_t,16> ops = createOpsArray();

    musqlite3_query insert_vector_query;
    musqlite3_query select_vector_query;
    musqlite3_query select_all_vector_query;

    musqlite3_query insert_rand_vector_query;
    musqlite3_query fetch_all_rand_vector_query;

    musqlite3_query select_metadata_query;
    musqlite3_query count_vector_query;

    std::array<musqlite3_query, 4> select_16CN_vectors_query;
    std::array<std::vector<uint16_t>,4> op_variations_16CN;
    //will create a Database if one doesn't exist
    explicit Database(const std::string& fileName, int vector_size = -1){
        _vector_size = vector_size;
        std::cout << sqlite3_libversion() << std::endl;
        openDatabase(fileName);
        initSqlQueries();

        validateTables();
        ensureCorrectVectorSize(vector_size);
        loadHashMatrix();
        generateOpLists();
    }

    ~Database(){
        int error_code = sqlite3_close(db);
        if (error_code != SQLITE_OK){
            std::cout<<sqlite3_errstr(error_code);
        }
    }

    void insertVector(const insert_query_input& in){
        if (in.vector.size() != _vector_size){
            std::cout<<"Catostrophic error. Vector size not the same on insert.";
            return;
        }
        uint16_t key = hashVector(in.vector.transpose(),hashMatrix);
        Vec normalized_vector = in.vector/in.vector.norm();

        sqlite3_bind_int(insert_vector_query.get(),1,key);
        sqlite3_bind_blob(insert_vector_query.get(),2,normalized_vector.data(),sizeof(float)*_vector_size,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_vector_query.get(),3,in.metadata.c_str(),-1,SQLITE_TRANSIENT);
        sqlite3_step(insert_vector_query.get());
        sqlite3_reset(insert_vector_query.get());
    }

    void insertJSON(const std::string& json){
        insertVector(convertToInputFromJson(json,_vector_size));
    }

    void putRandVector(){
        Vec vector = genRandVec(_vector_size);
        vector = vector/vector.norm();
        Vec normalized_vector = vector/vector.norm();

        sqlite3_bind_int(insert_vector_query.get(),1,0);
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
        sqlite3_reset(count_vector_query.get());
        return out;
    }

    int countVectorsBinding(){
        return countVectors(0,65535);
    }

    int countVectors(std::string table){
        std::string sql = "SELECT COUNT(*) FROM "+table;
        std::unique_ptr<std::string> out = easyQuery(sql);
        return std::stoi((*out));
    }

    fetch_query_output fetchNVectors(const Vec & queryVector, int N){
        std::priority_queue<SortHelperStruct, std::vector<SortHelperStruct>,HeapComparator> queue{};
        std::vector<TableRow> out_array(N);
        uint16_t query_key = hashVector(queryVector,hashMatrix);
        TableRow currRow;
        SortHelperStruct currStruct;

        for (int hamming_distance = 0; hamming_distance < 4; hamming_distance ++){
            std::vector<TableRow> query_result;
            musqlite3_batched_query_wrraper query_wrapper = {select_16CN_vectors_query[hamming_distance],0};
            bind_query_params(query_wrapper,op_variations_16CN[hamming_distance],query_key);

            while (sqlite3_step(query_wrapper.wrapped_query.get()) == SQLITE_ROW){
                currRow = getRowFromStep(query_wrapper.wrapped_query);
                currStruct = {currRow.vector.dot(queryVector),currRow};
                if (queue.size() == N){
                    if (queue.top().cosineIndex < currStruct.cosineIndex){
                        queue.pop();
                        queue.push(currStruct);
                    }
                }
                else{
                    queue.push(currStruct);
                }
            }

            if (queue.size() == N){
                sqlite3_reset(query_wrapper.wrapped_query.get());
                break;
            }
            sqlite3_reset(query_wrapper.wrapped_query.get());
        }
//        if (queue.size() != N){
//            while (sqlite3_step(select_all_vector_query.get()) == SQLITE_ROW){
//                currRow = getRowFromStep(select_all_vector_query);
//                currStruct = {currRow.vector.dot(queryVector),currRow};
//                if (queue.size() == N){
//                    if (queue.top().cosineIndex < currStruct.cosineIndex){
//                        queue.pop();
//                        queue.push(currStruct);
//                    }
//                }
//                else{
//                    queue.push(currStruct);
//                }
//            }
//            sqlite3_reset(select_all_vector_query.get());
//        }
        bool success = (queue.size()==N);
        for (int i = queue.size()-1; i >= 0; i--){
            out_array[i] = (queue.top().row);
            queue.top();
            queue.pop();
        }

        return {success,out_array};
    }

    fetch_query_output fetchVectorsRandomWalk(const Vec & queryVector, int N){
        Vec search;
        std::priority_queue<SortHelperStruct, std::vector<SortHelperStruct>,HeapComparator> queue{};
        std::vector<TableRow> out_array(N);
        TableRow currRow;
        SortHelperStruct currStruct;
        for (int i = 0; i < 10; i++){
            if (i !=0) {
                search = (queryVector + genRandVec(1536)*0.5);
            }
            else{
                search = queryVector;
            }
            search = search/search.norm();
            uint16_t search_key = generateKey(search);
            sqlite3_bind_int(select_16CN_vectors_query[0].get(),1,search_key);
            sqlite3_step(select_16CN_vectors_query[0].get());
            while (sqlite3_step(select_16CN_vectors_query[0].get()) == SQLITE_ROW){
                currRow = getRowFromStep(select_16CN_vectors_query[0]);
                currStruct = {currRow.vector.dot(queryVector),currRow};
                if (queue.size() == N){
                    if (queue.top().cosineIndex < currStruct.cosineIndex){
                        queue.pop();
                        queue.push(currStruct);
                    }
                }
                else{
                    queue.push(currStruct);
                }
            }
            sqlite3_reset(select_16CN_vectors_query[0].get());
        }
        bool success = (queue.size()==N);
        for (int i = queue.size()-1; i >= 0; i--){
            out_array[i] = (queue.top().row);
            queue.top();
            queue.pop();
        }
        return {success,out_array};
    }


    Vec ez_query(const Vec& queryVector){
        return fetchNVectors(queryVector,1).result[0].vector;
    }


    void read_query_output(musqlite3_query & query, std::vector<TableRow> & output){
        while (sqlite3_step(query.get()) == SQLITE_ROW){
            output.push_back(getRowFromStep(query));
        }
    }

    void bind_query_params(musqlite3_batched_query_wrraper & query, const std::vector<uint16_t> operations, const uint16_t & query_key){
        for (const uint16_t & op : operations){
            query.bind(query_key^op);
        }
    }

    void populate_query(musqlite3_batched_query_wrraper & query,const uint16_t & query_key, int min, int depth, uint16_t op){
        if (depth == 0){
            //insert op into array
            query.bind(query_key|op);
            return;
        }

        int max = 16-depth+1;
        for (int i = min; i <= max; i++){
            populate_query(query,query_key,i+1,depth-1,op^ops[i]);
        }
    }

    uint16_t generateKey(Vec & v){
        return hashVector(v,hashMatrix);
    }

    std::string getBitHashStr(Vec & v){
        return convertToString(generateKey(v));
    }

    std::string compareBitHashStr(Vec& v1, Vec& v2){
        return convertToString(generateKey(v1)^generateKey(v2));
    }

    int approxMedianBucketSize(){
        const int partitionsize = 512;
        double buckets[65536/partitionsize] = {};
        for (int i = 0; i < 65356/partitionsize; i++){
            buckets[i] = countVectors(partitionsize*i, partitionsize*(i+1)-1)/partitionsize;
        }
        std::sort(&buckets[0],&buckets[65536/partitionsize]);
        return buckets[65536/(partitionsize*2)];
    }

private:
    void loadHashMatrix(){
        VectorQueryOutput vecs = fetchRandomVectors();

        hashMatrix = composeHashMatrix(*vecs.rowList);
    }

    VectorQueryOutput fetchRandomVectors(){
        std::string sql = "SELECT * FROM random_vectors";
        VectorQueryOutput output = {_vector_size,std::make_unique<std::vector<TableRow>>()};
        easyQuery(sql,fetchVectorsCallback,&output);

        return output;
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
                                                        "    key INT,\n"
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
                Vec rand = Eigen::VectorXf::Random(_vector_size);
                rand = rand / rand.norm();
                putVectorTable(i,rand,"random_vectors");
            }
        }
    }
    void putVectorTable(int key,const Vec& vector,std::string table,std::string metadata = ""){
        if (vector.size() != _vector_size){
            std::cout<<"Catostrophic error. Vector size not the same on insert.";
            return;
        }
        std::string sql = "INSERT INTO "+table+"(key,vector,metadata) VALUES("+std::to_string(key)+",'"+ convertToString(vector)+"','"+metadata+"')";
        easyQuery(sql,NULL,NULL);
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
        initSqlQuery("SELECT * FROM vectors",select_all_vector_query);

        initSqlQuery("SELECT * FROM rand_vectors",fetch_all_rand_vector_query);
        initSqlQuery("INSERT INTO vectors(key,vector,metadata) VALUES(?,?,?)",insert_vector_query);
        initSqlQuery("SELECT COUNT(*) FROM vectors WHERE key>=? AND key<=?",count_vector_query);

        for (int i = 0; i < 4; i++){
            create16CN_query(select_16CN_vectors_query[i], i);
        }
    }

    void initSqlQuery(const char * sql, musqlite3_query& query){
        sqlite3_stmt * raw_query;
        sqlite3_prepare_v2(db,sql,-1,&raw_query,nullptr);
        query = musqlite3_query(raw_query);
    }

    void create16CN_query(musqlite3_query& query,int n){
        std::string sql_text = "SELECT * FROM vectors WHERE key IN (";
        int options = combi(16,n);
        for (int i = 0; i < options; ++i) {
            sql_text += "?";
            if (i < options - 1) {
                sql_text += ", ";
            }
        }
        sql_text += ");";
        initSqlQuery(sql_text.c_str(), query);
    }

    TableRow getRowFromStep(const musqlite3_query & query){
        int id = sqlite3_column_int(query.get(),0);
        Vec v = convertToVecFromBLOB(sqlite3_column_blob(query.get(),1),_vector_size);
        std::string meta = "";
        meta = std::string(reinterpret_cast<const char *>(sqlite3_column_text(query.get(), 2)));
        TableRow new_row = {id,v,meta};
        return new_row;
    }

    void generateOpLists(){
        for (int i = 0; i < op_variations_16CN.size(); i++){
            op_variations_16CN[i] = generate_16CN_operations(ops,i);
        }
    }


};
