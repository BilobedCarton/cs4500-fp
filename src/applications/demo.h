#pragma once

#include "../client/application.h"
#include "../dataframe/dataframe.h"
#include "../store/key.h"

class Demo : public Application {
public:
  Key* main = new Key("main", 0);
  Key* verify = new Key("verif", 0);
  Key* check = new Key("ck", 0);
 
  Demo(size_t idx): Application(idx) {}
 
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
    delete DataFrame::fromArray(main, &kv, SZ, vals);
    delete DataFrame::fromScalar(check, &kv, sum);
    pln("Producer done.");
  }
 
  void counter() {
    DataFrame* v = DataFrame::deserialize(kv.waitAndGet(main)->serialized());
    pln("Counter got frame.");
    double sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    delete DataFrame::fromScalar(verify, &kv, sum);
    pln("Counter done.");
  }
 
  void summarizer() {
    DataFrame* result = DataFrame::deserialize(kv.waitAndGet(verify)->serialized());
    DataFrame* expected = DataFrame::deserialize(kv.waitAndGet(check)->serialized());
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
    pln("Summarizer done.");
  }
};