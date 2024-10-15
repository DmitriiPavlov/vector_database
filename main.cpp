//builtin
#include <iostream>
#include <string>

//external
#include <Eigen/Dense>

//internal
#include "database.h"
#include "conversion.h"
#include "locality_hashing.h"


int main(int argc, char** argv)
{
    Database* db = new Database("/Users/bison/Documents/Personal Projects/vectorDatabase/data/tryagain.db",1536);


    for (int i = 0; i < 1000; i++) {
        Vec a = Eigen::VectorXf::Random(1536);
        db->putVector(a,"");
    }



    std::vector<TableRow> result;
    result = *(db->fetchVectors(0,60000).rowList);

    for (TableRow& r : result){
        std::cout<<r.vector<<std::endl;
    }
    std::cout<<result.size();
    free(db);
}
