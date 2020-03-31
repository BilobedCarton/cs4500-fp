#pragma once

#include "../utils/object.h"
#include "../dataframe/dataframe.h"
#include "../store/key.h"
#include "../store/kvstore.h"
#include "../utils/thread.h"


static KVStore kv;

class Application : public Object {
public:

    size_t idx_;

    Application(size_t idx) {
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
    node->run_();
  }
};