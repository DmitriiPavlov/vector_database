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

    clock_t start = clock();
//    for (int i = 0; i < 10000; i++) {
//        if (i%10000 == 0){
//            std::cout<<"Ten thousand inserts.\n";
//        }
//        Vec a = Eigen::VectorXf::Random(1536);
//        db->putVector(a,"");
//    }


      std::cout<<db->countVectors(0,70356);
//    std::vector<TableRow> result;
//    Vec search;
//    for (int i = 0; i < 1000; i++) {
//        search = Eigen::VectorXf::Random(1536);
//        search = search / search.norm();
//        result = (db->fetchClosestVectors(search, 100));
//    }
//    for (TableRow& r : result){
//        std::cout<<r.vector<<"\n";
//        std::cout<<r.vector.dot(search)<<"\n";
//    }
//    clock_t stop = clock();
//    std::cout<<result.size()<<"\n";
//    std::cout<<result[1].vector.norm()<<"\n";
//    double elapsed = (double) (stop - start) / CLOCKS_PER_SEC;
//    std::cout<<elapsed;
    free(db);
}
