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
//        db->putVector(a,"");
//    }

//
//    std::cout<<db->countVectors(0,70356);
    Vec search =  Eigen::VectorXf::Random(1536);
    search = search/search.norm();
    std::vector<TableRow> result;
    for (int i = 0; i < 1000; i ++){
        result = db->fetchNVectors(search,10);
    }
//    for (int i = 0; i < 100; i++){
//        TableRow row = result[i];
//        std::cout<<row.vector<<std::endl;
//        std::cout<<row.id<<"\n";
//        std::cout<<row.vector.dot(search)<<"\n";
//    }
    clock_t stop = clock();
    double elapsed = (double) (stop - start) / CLOCKS_PER_SEC;
    std::cout<<elapsed<<std::endl;
    std::cout<<1000/elapsed<<std::endl;
    free(db);
}
