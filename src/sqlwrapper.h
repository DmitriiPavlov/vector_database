
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
    sqlite3* db = nullptr;
    int _vector_size = -1;
    int _random_vector_amount = -1;
    //here as the only two performance intensive queries - all the other ones may just be generated
    //on the fly
    musqlite3_query insert_vector_query;
    musqlite3_query select_vector_query;

public:
    //this creates connection to sql database if file exists
    //throws error if file doesn't exist
    explicit InternalSQLWrapper(const std::string& filename, int vector_size, int random_vector_amount){
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

        _vector_size = vector_size;
        _random_vector_amount = random_vector_amount;
        initSqlQuery(db,"SELECT * FROM vectors WHERE key=?",select_vector_query);
        initSqlQuery(db,"INSERT INTO vectors(key,vector,metadata) VALUES(?,?,?)",insert_vector_query);
    }
    //key datatype subject to change for flexibility
    void insertVector(int key, const Vec& vector, const std::string& metadata){
        if (vector.size()!= _vector_size){
            throw std::runtime_error("Attempting to insert vector of invalid size!");
        }

        sqlite3_bind_int(insert_vector_query.get(),1,key);
        sqlite3_bind_blob(insert_vector_query.get(),2,vector.data(),sizeof(float)*_vector_size,SQLITE_TRANSIENT);
        sqlite3_bind_text(insert_vector_query.get(),3,metadata.c_str(),-1,SQLITE_TRANSIENT);
        sqlite3_step(insert_vector_query.get());
        sqlite3_reset(insert_vector_query.get());
    }

    //potentially slow, avoiding premature optimization
    std::vector<TableRow> selectTableRows(int key){
        std::vector<TableRow> out;
        sqlite3_bind_int(select_vector_query.get(),1,key);
        while (sqlite3_step(select_vector_query.get())==SQLITE_ROW){
            out.push_back(getRowFromStep());
        }
        sqlite3_reset(select_vector_query.get());
        return out;
    }

    //this is used to initialize the sql database, but doesn't create the connection
    static void init(std::string filename, int vector_size, int random_vector_amount){
        sqlite3* temp_db = nullptr;
        int error_code = sqlite3_open_v2(filename.c_str(), &temp_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

        if (error_code != SQLITE_OK){
            throw std::runtime_error("Error initiliazing database!");
        }

        runStringQuery(temp_db,"CREATE TABLE IF NOT EXISTS vectors (\n"
                               "    key INT NOT NULL,\n"
                               "    vector BLOB NOT NULL,\n"
                               "     metadata TEXT\n"
                               ");");
        runStringQuery(temp_db,"CREATE INDEX IF NOT EXISTS idx_hash ON vectors(key);");

        runStringQuery(temp_db,"CREATE TABLE IF NOT EXISTS random_vectors (\n"
                       "    key INT,\n"
                       "    vector BLOB NOT NULL,\n"
                       ");");

        runStringQuery(temp_db,"CREATE TABLE IF NOT EXISTS metadata (\n"
                       "    key TEXT NOT NULL,\n"
                       "    value INT NOT NULL\n"
                       ");");

        insertMetadata(temp_db,"vector_size", vector_size);
        insertMetadata(temp_db,"random_vector_amount",random_vector_amount);
        createRandomVectorTable(temp_db,vector_size,random_vector_amount);

        sqlite3_close_v2(temp_db);
    }

    static bool dbExists(const std::string& filename, int vector_size){
        return std::filesystem::exists(filename);
    }

    ~InternalSQLWrapper(){
        int error_code = sqlite3_close(db);
        if (error_code != SQLITE_OK){
            std::cout<<"Error closing database, "<<sqlite3_errstr(error_code);
        }
    }

private:
    static void insertMetadata(sqlite3* sqldb, std::string key, int value){
        std::string sql = "INSERT INTO metadata(key,value) VALUES('"+key+"'," + std::to_string(value) + ");";
        runStringQuery(sqldb,sql);
    }

    static void createRandomVectorTable(sqlite3* sqldb, int vector_size,int random_vector_amount){
        for (int i = 0; i < random_vector_amount; i++) {
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

    std::vector<Vec> getAllRandomVectors(){
        musqlite3_query query;
        std::vector<Vec> outVec;
        initSqlQuery(db,"SELECT * FROM random_vectors",query);
        while(sqlite3_step(query.get()) == SQLITE_ROW){
            outVec.push_back(convertToVecFromBLOB(sqlite3_column_blob(query.get(),2),_vector_size));
        }
        return outVec;
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

    TableRow getRowFromStep(){
        int id = sqlite3_column_int(select_vector_query.get(),0);
        Vec v = convertToVecFromBLOB(sqlite3_column_blob(select_vector_query.get(),1),_vector_size);
        std::string meta = "";
        meta = std::string(reinterpret_cast<const char *>(sqlite3_column_text(select_vector_query.get(), 2)));
        TableRow new_row = {id,v,meta};
        return new_row;
    }


    static void runStringQuery(sqlite3* sqldb, std::string query){
        int error_code = sqlite3_exec(sqldb,query.c_str(),NULL,NULL,NULL);
        if (error_code != SQLITE_OK){
            throw std::runtime_error(std::string(sqlite3_errstr(error_code))+" from : "+query);
        }
    }



};


#endif

