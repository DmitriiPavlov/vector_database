#pragma once
//builtin
#include <string>
//external
#include "Eigen/Dense"
#include "yyjson.h"
//internal
#include "types.h"

#ifndef VECTORDATABASE_CONVERSION_H
#define VECTORDATABASE_CONVERSION_H

#endif //VECTORDATABASE_CONVERSION_H


std::string convertToString(Vec vector){
    std::stringstream ss;
    for (int i = 0; i < vector.size(); i++){
        ss<<vector[i];
        if (i!=vector.size()-1){
            ss<<" ";
        }
    }
    return ss.str();
}

Vec convertToVec(std::string str, int size){
    std::istringstream iss(str);
    Vec out(size);
    float num;
    for (int i = 0; i < size; i++){
        iss>>num;
        out[i] = num;
    }
    return out;
}

Vec convertToVecFromBLOB(const void * blobPointer, int vec_size){
    Vec outVec(vec_size);
    std::memcpy(outVec.data(),blobPointer,sizeof(float)*vec_size);
    return outVec;
}

insert_query_input convertToInputFromJson(const std::string& json, int size){
    yyjson_doc * doc = yyjson_read(json.data(),json.length(),0);
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val * metadata = yyjson_obj_get(root,"metadata");
    yyjson_val * vec_array= yyjson_obj_get(root, "vector");
    size_t idx, max;
    yyjson_val *vec_val;
    size_t arr_size = yyjson_arr_size(vec_array);

    insert_query_input out = {yyjson_get_str(metadata), Vec(size)};
    yyjson_arr_foreach(vec_array, idx, max, vec_val) {
        out.vector[idx] = (float)yyjson_get_real(vec_val);
    }

    yyjson_doc_free(doc);
    return out;
}

std::string convertToJsonFromOutput(const fetch_query_output& output){
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "success", output.success ? "true" : "false");

    yyjson_mut_val* output_array = yyjson_mut_arr(doc);
    for (const TableRow& r : output.result){
        yyjson_mut_val* row = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc,row,"metadata",r.metadata.c_str());

        yyjson_mut_val* vector_array = yyjson_mut_arr(doc);
        for (const float& val : r.vector) {
            yyjson_mut_arr_add_float(doc,vector_array,val);
        }
        yyjson_mut_obj_add_val(doc, row, "vector", vector_array);
        yyjson_mut_arr_add_val(output_array,row);
    }

    yyjson_mut_obj_add_val(doc,root,"vectors",output_array);

    const char *json_str = yyjson_mut_write(doc, 0, nullptr);
    yyjson_mut_doc_free(doc);

    return json_str;
}


