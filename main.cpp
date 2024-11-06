//builtin
#include <iostream>
#include <string>
#include <time.h>
//external
#include <Eigen/Dense>

//internal
#include "database.h"
#include "conversion.h"
#include "locality_hashing.h"

int main(int argc, char** argv)
{
    Database* db = new Database("/Users/bison/Documents/Personal Projects/vectorDatabase/data/tryagain.db",1536);
    std::cout.setf( std::ios_base::unitbuf );
    clock_t start = clock();
//    for (int i = 0; i < 10000; i++) {
//        if (i%10000 == 0){
//            std::cout<<"Ten thousand inserts.\n";
//        }
//        Vec a = Eigen::VectorXf::Random(1536);
//        db->insertVector(a,"");
//    }

//
    std::cout<<db->countVectors(0,70356);
    Vec search =  Eigen::VectorXf::Random(1536);
    search = search/search.norm();
    fetch_query_output output;

    for (int i = 0; i < 1000; i ++){
        output = db->fetchNVectors(search,2);
    }
    std::cout<<convertToJsonFromOutput(output);

    insert_query_input trial = convertToInputFromJson("{\n"
                                                      "    \"metadata\" : \"Trial Vec\",\n"
                                                      "    \"vector\": [1.013,1.0141,0.41141]\n"
                                                      "}",3);

    std::cout<<trial.vector<<"\n";
    std::cout<<trial.metadata;

    clock_t stop = clock();
    double elapsed = (double) (stop - start) / CLOCKS_PER_SEC;
    std::cout<<elapsed<<std::endl;
    std::cout<<1000/elapsed<<std::endl;
    free(db);
}
