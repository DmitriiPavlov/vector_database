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
#include "src/client.h"
int main(int argc, char** argv) {
    DatabaseClient db = DatabaseClient("/Users/bison/Documents/Personal Projects/vectorDatabase/datadifferentkeytest_20keys", 1536,20);
//    for (int i = 0; i < 150000; i++){
//        if (i%1000 == 0){
//            std::cout<<i<<std::endl;
//        }
//        db.insertVector(genRandVec(1536),"");
//    }

    Vec test_vectors[1000];
    Vec corrupted_vectors[1000];
    for (int i = 0; i < 100; i++){
        test_vectors[i] = genRandVec(1536);
        for (int j = 0; j < 10; j++){
            corrupted_vectors[i] = test_vectors[i] + 1*genRandVec(1536);
            corrupted_vectors[i] = corrupted_vectors[i]/corrupted_vectors[i].norm();
            db.insertVector(corrupted_vectors[i],"");
        }
    }

    db.syncBuffer();
    std::cout<<db.total_vector_amount<<"\n";

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 100; i++){
        auto result = db.fetchNVectors(test_vectors[i],1,-1.0f);
    }
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> duration = end - start;
    std::cout << "Time taken: " << duration.count() << " seconds" << std::endl;
    std::cout << "Average time taken: " << duration.count()/100 << " seconds" << std::endl;
    std::cout<<100/duration.count()<<"\n";
}