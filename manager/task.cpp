#include "task.hpp"

namespace husky {

BinStream & operator>>(BinStream & stream, Task & task) {
    size_t len;
    stream >> len;
    for(int i=0; i<len; i++) {
        Operation op;
        stream >> op;
        task.op_list.push_back(op);
    }
}

}
