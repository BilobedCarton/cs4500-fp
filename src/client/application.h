#pragma once

#include "../utils/object.h"
#include "../dataframe/dataframe.h"
#include "../store/key.h"
#include "../store/kvstore.h"

class Application : public Object {
public:
    size_t idx_;
    KVStore* kv;

    Application(size_t idx) {
        idx_ = idx;
    }

    virtual void run_();

    size_t this_node() {
      return idx_;
    }
};