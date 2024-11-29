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
    DatabaseClient db = DatabaseClient("/Users/bison/Documents/Personal Projects/vectorDatabase/data/deviltest1536_pt2", 1536,5);
//    for (int i = 0; i < 50000; i++) {
//        db.insertVector(genRandVec(1536), "");
//    }
    Vec search = genRandVec(1536);
    Vec corrupted_search = search+ genRandVec(1536)*1;
    corrupted_search = corrupted_search/corrupted_search.norm();
    db.insertVector(search,"");
    std::cout<<search.dot(corrupted_search)<<"\n";
    auto result = db.fetchNVectors(corrupted_search, 1, 0.40);
    std::cout<<result[0].second<<"\n";
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 1000; i++) {
//        search = genRandVec(1536);
//        corrupted_search = search + genRandVec(1536)*1;
        result = db.fetchNVectors(corrupted_search, 1, 0.40);
    }

    std::cout<<result[0].second<<"\n";
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> duration = end - start;
    std::cout << "Time taken: " << duration.count() << " seconds" << std::endl;
    std::cout << "Average time taken: " << duration.count()/1000 << " seconds" << std::endl;
    std::cout<<1000/duration.count()<<"\n";
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 1000; i++) {
        search = genRandVec(1536);
        corrupted_search = search + genRandVec(1536)*1;
        result = db.fetchNVectors(corrupted_search, 1, 0.40);
    }

    std::cout<<result[0].second<<"\n";
    end = std::chrono::high_resolution_clock::now();

    duration = end - start;
    std::cout << "Time taken: " << duration.count() << " seconds" << std::endl;
    std::cout << "Average time taken: " << duration.count()/1000 << " seconds" << std::endl;
    std::cout<<1000/duration.count();
}