//builtin
#include <iostream>
#include <string>

//external
#include <Eigen/Dense>

//internal
#include "queries.h"
#include "conversion.h"
#include "locality_hashing.h"


int main(int argc, char** argv)
{
    Database* db = new Database("/Users/bison/Documents/Personal Projects/vectorDatabase/data/tryagain.db",1536);


//    for (int j = 0; j < 100; j++) {
//        db->beginInsertVectorBatch();
//        for (int i = 0; i < 1000; i++) {
//            Vec a = Eigen::VectorXf::Random(1536);
//            db->putVectorInBatch(a, "");
//        }
//        db->endInsertVectorBatch();
//    }



    std::vector<TableRow> result;
    Vec search;
    for (int i = 0; i < 5000; i++){
        Vec a = Eigen::VectorXf::Random(1536);
        Vec b = Eigen::VectorXf::Random(1536);
        b = b/b.norm();
        search = (a+b)/2;
        search = search/search.norm();
        result = db->fetchClosestVectors(search,5);
    }
    std::cout<<search;
    for (TableRow& r : result){
        std::cout<<r.vector<<std::endl;
        std::cout<<r.vector.dot(search)<<std::endl;
    }
    std::cout<<(double)(db->average_match_index)/5000;

    free(db);
}
