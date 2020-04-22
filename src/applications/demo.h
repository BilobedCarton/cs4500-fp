#pragma once

#include "../client/application.h"
#include "../dataframe/distributed_dataframe.h"
#include "../store/key.h"

class Demo : public Application {
public:
  Key* main = new Key("main", 0);
  Key* verify = new Key("verif", 0);
  Key* check = new Key("ck", 0);
 
  Demo(size_t idx, NetworkIfc* net): Application(idx, net) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    size_t SZ = 100*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    delete DistributedDataFrame::fromArray(main, &kv, SZ, vals);
    delete DistributedDataFrame::fromScalar(check, &kv, sum);
    Logger::log("Producer done.");
  }
 
  void counter() {
    DistributedDataFrame* v = DistributedDataFrame::deserialize(kv.waitAndGet(main)->serialized(), &kv);
    Logger::log("Counter got frame.");
    double sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum); // TODO: use logger
    delete DistributedDataFrame::fromScalar(verify, &kv, sum);
    Logger::log("Counter done.");
  }
 
  void summarizer() {
    DistributedDataFrame* result = DistributedDataFrame::deserialize(kv.waitAndGet(verify)->serialized(), &kv);
    DistributedDataFrame* expected = DistributedDataFrame::deserialize(kv.waitAndGet(check)->serialized(), &kv);
    Logger::log(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
    Logger::log("Summarizer done.");
  }
};