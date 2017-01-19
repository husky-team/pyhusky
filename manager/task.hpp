#pragma once
#include "husky/base/serialization.hpp"
#include "operation.hpp"

namespace husky {

using base::BinStream;

class Task;
BinStream & operator>>(BinStream & stream, Task & task);

class Task {
public:
    friend BinStream & operator>>(BinStream & stream, Task & task);
    size_t get_num_operations() {
        return op_list.size();
    }
    Operation & get_operation(int idx) {
        return op_list[idx];
    }
protected:
    std::vector<Operation> op_list;
};


}
