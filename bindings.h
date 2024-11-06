#pragma once
#include "pybind11/pybind11.h"
#include "pybind11/eigen.h"
#include "database.h"

namespace py = pybind11;

PYBIND11_MODULE(ezdb,module){
    py::class_<Database>(module, "Database")
            .def(py::init<const std::string&, int>(), py::arg("fileName"), py::arg("vector_size") = -1)
            .def("insert", &Database::insertJSON, py::arg("json"))
            .def("query", &Database::ez_query, py::arg("query_vec"))
            .def("query", [](Database &self, py::array_t<float> query_vec) {
                // Convert the Python NumPy array to Eigen::VectorXf
                py::buffer_info buf_info = query_vec.request();
                float* ptr = static_cast<float*>(buf_info.ptr);
                Eigen::Map<Eigen::VectorXf> vec(ptr, buf_info.size);
                return self.ez_query(vec);  // Call the actual method
            });
}
