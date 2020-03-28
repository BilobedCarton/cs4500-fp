#pragma once
#include "../dataframe/dataframe.h"

class Visitor {
public:
    virtual bool done();
};

class Writer : public Visitor {
public:
    virtual void visit(Row& r);
};

class Reader : public Visitor {
public:
    virtual bool visit(Row& r);
};