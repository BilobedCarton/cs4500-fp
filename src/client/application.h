#pragma once

#include "../utils/object.h"
#include "../dataframe/dataframe.h"
#include "../store/key.h"
#include "../store/kvstore.h"
#include "../utils/thread.h"

class Application : public Object {
public:
    KVStore kv;
    size_t idx_;

    Application(size_t idx, NetworkIfc* net) : kv(idx, net) {
        idx_ = idx;
    }

    virtual void run_() { return; }

    size_t this_node() {
      return idx_;
    }
};

class NodeThread : public Thread {
public:
  Application* node; // not owned

  NodeThread(Application* n) {
    node = n;
  }

  void run() {
    node->kv.network_->register_node(node->idx_);
    node->run_();
  }
};