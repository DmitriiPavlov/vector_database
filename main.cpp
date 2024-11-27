//builtin
#include <iostream>
#include <string>
#include <time.h>
//external
#include <Eigen/Dense>

//internal
#include "src/database.h"
#include "src/conversion.h"
#include "src/locality_hashing.h"

int main(int argc, char** argv)
{
    Database* db = new Database("/Users/bison/Documents/Personal Projects/vectorDatabase/data/westartanew.db",1536);
    std::cout.setf( std::ios_base::unitbuf );
//    for (int i = 0; i < 100000; i++) {
//        if (i%10000 == 0){
//            std::cout<<"Ten thousand inserts.\n";
//        }
//        Vec a = genRandVec(db->_vector_size);
//        db->insertVector({"",a});
//    }

    std::cout<<db->countVectorsBinding()<<"\n";
    std::cout<<db->approxMedianBucketSize()<<"\n";

//
//    std::cout<<db->countVectors(0,70356);
    Vec search =  Eigen::VectorXf::Random(1536);
    search = genRandVec(1536);
//    std::cout<<search;
    search = search/search.norm();
    fetch_query_output output;

    clock_t start = clock();
    Vec corrupted_search = search +  genRandVec(1536);
    corrupted_search = corrupted_search/corrupted_search.norm();
    std::cout<<db->getBitHashStr(corrupted_search)<<"\n";
    std::cout<<db->getBitHashStr(search)<<"\n";
    std::cout<<db->compareBitHashStr(search,corrupted_search)<<"\n";
    std::cout<<search.dot(corrupted_search)<<"\n";
    db->insertVector({"",corrupted_search});
    for (int i = 0; i < 4000; i ++){
        output = db->fetchNVectors(search,10);
    }
    clock_t stop = clock();
//    std::cout<<convertToJsonFromOutput(output)<<"\n";
    std::cout<<output.result[0].vector.dot(search)<<"\n";
    std::cout<<output.success<<"\n";
    std::cout<<convertToString(db->generateKey(output.result[0].vector))<<"\n";
    std::cout<<convertToString(db->generateKey(search))<<"\n";
    insert_query_input trial = convertToInputFromJson("{\n"
                                                      "    \"metadata\" : \"Trial Vec\",\n"
                                                      "    \"vector\": [1.013,1.0141,0.41141]\n"
                                                      "}",3);

//    std::cout<<trial.vector<<"\n";
//    std::cout<<trial.metadata;

    double elapsed = (double) (stop - start) / CLOCKS_PER_SEC;
    std::cout<<elapsed<<std::endl;
    std::cout<<1000/elapsed<<std::endl;
    free(db);
}
