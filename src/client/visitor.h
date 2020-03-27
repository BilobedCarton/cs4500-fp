#pragma once
#include "../dataframe/dataframe.h"

class Visitor {
public:
    virtual void visit(Row& r);
    virtual bool done();
};

class Writer : public Visitor {
public:

};

class Reader : public Visitor {
public:

};