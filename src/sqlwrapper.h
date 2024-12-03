
#ifndef VECTORDATABASE_SQLWRAPPER_H
#define VECTORDATABASE_SQLWRAPPER_H
#pragma once
//builtin
#include <string>
#include <iostream>
#include <format>
#include <filesystem>

//external
#include "sqlite3.h"
#include "Eigen/Dense"

//internal
#include "types.h"
#include "conversion.h"
#include "locality_hashing.h"
#include "rng.h"



class InternalSQLWrapper{
    //here as the only two performance intensive queries - all the other ones may just be generated
    //on the fly
    sqlite3* db = nullptr;
    musqlite3_query insert_vector_query;
    musqlite3_query insert_50_vector_query;
    std::vector<musqlite3_query> select_vector_queries;


public:
    int _vector_size = -1;
    int _random_vector_amount = -1;
    int _key_count = -1;
    const int _batch_size = 500;
    //this creates connection to sql database if file exists
    //throws error if file doesn't exist
    explicit InternalSQLWrapper(const std::string& filename, int vector_size, int random_vector_amount, int key_count){
        int error_code = sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE, NULL);

        if (error_code == SQLITE_CANTOPEN){
            throw std::runtime_error("Database " + filename +" doesn't exist!");
        }
        if (error_code != SQLITE_OK){
            throw std::runtime_error("Error opening database!");
        }

        if (getMetadata("vector_size")!= vector_size){
            throw std::runtime_error("Invalid preexisting vector size in file: "+std::to_string(getMetadata("vector_size")));
        }

        if(getMetadata("random_vector_amount")!=random_vector_amount){
            throw std::runtime_error("Invlaid preexisting random vector amount in file: "+std::to_string(getMetadata("random_vector_amount")));
        }

        if(getMetadata("key_count")!= key_count){
            throw std::runtime_error("Invlaid preexisting key count amount in file: "+std::to_string(getMetadata("key_count")));
        }

        _vector_size = vector_size;
        _random_vector_amount = random_vector_amount;
        _key_count = key_count;
        initDynamicQueries();
    }
    //key datatype subject to change for flexibility
    void insert(const std::vector<int>& keys, const Vec& vector, const std::string& metadata){
        if (vector.size()!= _vector_size){
            throw std::runtime_error("Attempting to insert vector of invalid size!");
        }

        for (int i = 0; i < _key_count; i++){
            sqlite3_bind_int(insert_vector_query.get(),i+1,keys[i]);
        }

        sqlite3_bind_blob(insert_vector_query.get(),_key_count+1,vector.data(),sizeof(float)*_vector_size,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_vector_query.get(),_key_count+2,metadata.c_str(),-1,SQLITE_TRANSIENT);
        sqlite3_step(insert_vector_query.get());
        sqlite3_reset(insert_vector_query.get());
    }


    //potentially slow, avoiding premature optimization
    std::vector<TableRow> select(int key, int keynum){
        std::vector<TableRow> out;
        sqlite3_bind_int(select_vector_queries[keynum].get(),1,key);
        while (sqlite3_step(select_vector_queries[keynum].get())==SQLITE_ROW){
            out.push_back(getRowFromStep(keynum));
        }
        sqlite3_reset(select_vector_queries[keynum].get());
        return out;
    }

    void beginSelect(int key, int keynum){
        sqlite3_bind_int(select_vector_queries[keynum].get(),1,key);
    }

    TableRow stepSelect(int keynum){
        TableRow out;
        if (sqlite3_step(select_vector_queries[keynum].get())==SQLITE_ROW){
            out = getRowFromStep(keynum);
        }
        else{
            sqlite3_reset(select_vector_queries[keynum].get());
        }
        return out;
    }

    void finishSelect(int keynum){
        sqlite3_reset(select_vector_queries[keynum].get());
    }
    //this is used to initialize the sql database, but doesn't create the connection
    static void init(std::string filename, int vector_size, int random_vector_amount, int key_count){
        sqlite3* temp_db = nullptr;
        int error_code = sqlite3_open_v2(filename.c_str(), &temp_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

        if (error_code != SQLITE_OK){
            throw std::runtime_error("Error initiliazing database!");
        }

        createTableWithIndices(temp_db,key_count);

        runStringQuery(temp_db,"CREATE TABLE IF NOT EXISTS random_vectors (\n"
                       "    key INT,\n"
                       "    vector BLOB NOT NULL\n"
                       ");");

        runStringQuery(temp_db,"CREATE TABLE IF NOT EXISTS metadata (\n"
                       "    key TEXT NOT NULL,\n"
                       "    value INT NOT NULL\n"
                       ");");

        insertMetadata(temp_db,"vector_size", vector_size);
        insertMetadata(temp_db,"random_vector_amount",random_vector_amount);
        insertMetadata(temp_db,"key_count",key_count);
        createRandomVectorTable(temp_db,vector_size,random_vector_amount, key_count);

        sqlite3_close_v2(temp_db);
    }

    static bool dbExists(const std::string& filename){
        return std::filesystem::exists(filename);
    }

    ~InternalSQLWrapper(){
        //to prevent locking, must release queries
        for (musqlite3_query& query : select_vector_queries){
            query = nullptr;
        }
        insert_vector_query = nullptr;

        int error_code = sqlite3_close(db);
        if (error_code != SQLITE_OK){
            std::cout<<"Error closing database, "<<sqlite3_errstr(error_code);
        }
    }

    std::vector<Vec> getAllRandomVectors(){
        musqlite3_query query;
        std::vector<Vec> outVec;
        initSqlQuery(db,"SELECT * FROM random_vectors",query);
        while(sqlite3_step(query.get()) == SQLITE_ROW){
            outVec.push_back(convertToVecFromBLOB(sqlite3_column_blob(query.get(),1),_vector_size));
        }
        return outVec;
    }

    int getTotalCount(){
        musqlite3_query query;
        initSqlQuery(db,std::string("SELECT COUNT(*) FROM vectors").c_str(),query);
        sqlite3_step(query.get());
        return sqlite3_column_int(query.get(),0);
    }

private:
    static void insertMetadata(sqlite3* sqldb, std::string key, int value){
        std::string sql = "INSERT INTO metadata(key,value) VALUES('"+key+"'," + std::to_string(value) + ");";
        runStringQuery(sqldb,sql);
    }

    static void createRandomVectorTable(sqlite3* sqldb, int vector_size,int random_vector_amount, int key_count){
        for (int i = 0; i < random_vector_amount*key_count; i++) {
            Vec rand = genRandVec(vector_size);
            rand = rand / rand.norm();
            insertRandomVector(sqldb,i,rand,vector_size);
        }
    }

    static void insertRandomVector(sqlite3* sqldb,int key, const Vec& v,int vector_size){
        musqlite3_query query;
        initSqlQuery(sqldb,"INSERT INTO random_vectors(key, vector) VALUES(?,?)",query);
        sqlite3_bind_int(query.get(),1,key);
        sqlite3_bind_blob(query.get(),2,v.data(),sizeof(float)*vector_size,SQLITE_TRANSIENT);
        sqlite3_step(query.get());
        sqlite3_reset(query.get());
    }


    int getMetadata(std::string key){
        musqlite3_query query;
        initSqlQuery(db,std::string("SELECT DISTINCT value FROM metadata WHERE key='"+key+"'").c_str(),query);
        sqlite3_step(query.get());
        return sqlite3_column_int(query.get(),0);
    }

    static void initSqlQuery(sqlite3 * db, const char * sql, musqlite3_query& query){
        sqlite3_stmt * raw_query;
        sqlite3_prepare_v2(db,sql,-1,&raw_query,nullptr);
        query = musqlite3_query(raw_query);
    }

    void runStringQuery(std::string query){
        int error_code = sqlite3_exec(db,query.c_str(),NULL,NULL,NULL);
        if (error_code != SQLITE_OK){
            throw std::runtime_error(std::string(sqlite3_errstr(error_code))+" from : "+query);
        }
    }

    TableRow getRowFromStep(int keynum){
        Vec v = convertToVecFromBLOB(sqlite3_column_blob(select_vector_queries[keynum].get(),_key_count),_vector_size);
        std::string meta = "";
        meta = std::string(reinterpret_cast<const char *>(sqlite3_column_text(select_vector_queries[keynum].get(), _key_count+1)));
        TableRow new_row = {true,v,meta};
        return new_row;
    }


    static void runStringQuery(sqlite3* sqldb, std::string query){
        int error_code = sqlite3_exec(sqldb,query.c_str(),NULL,NULL,NULL);
        if (error_code != SQLITE_OK){
            throw std::runtime_error(std::string(sqlite3_errstr(error_code))+" from : "+query);
        }
    }

    static void createTableWithIndices(sqlite3 * db, int key_count){
        std::string sql =  "CREATE TABLE IF NOT EXISTS vectors (\n";
        for (int i = 0; i < key_count; i++){
            sql += "    key"+std::to_string(i)+" INT NOT NULL,\n";
        }
        sql+= "    vector BLOB NOT NULL,\n"
              "     metadata TEXT\n"
              ");";
        runStringQuery(db, sql);
        for (int i = 0; i < key_count; i++){
            runStringQuery(db, "CREATE INDEX IF NOT EXISTS idx_hash"+std::to_string(i)+" ON vectors(key"+std::to_string(i)+");");
        }
    }

    void initDynamicQueries(){
        select_vector_queries.resize(_key_count);
        for (int i = 0; i < _key_count; i++) {
            initSqlQuery(db, std::string("SELECT * FROM vectors WHERE key"+std::to_string(i)+"=?").c_str(), select_vector_queries[i]);
        }


        std::string initial_sql = "INSERT INTO vectors(";
        for (int i = 0; i < _key_count; i++){
            initial_sql+="key"+std::to_string(i)+",";
        }

        initial_sql+= "vector,metadata)";
        std::string values_sql = " VALUES(";

        for (int i = 0; i < _key_count; i++){
            values_sql+="?,";
        }
        values_sql += "?,?)";

        initSqlQuery(db,(initial_sql+values_sql).c_str(),insert_vector_query);

        std::string batch_sql = initial_sql;
        for (int i = 0; i < _batch_size; i++){
            batch_sql += values_sql;
            if (i!=_batch_size-1){
                batch_sql+=",";
            }
        }
        initSqlQuery(db,batch_sql.c_str(),insert_50_vector_query);

    }

};


#endif

