#include <assert.h>

#include "../test.h"
#include "../../src/utils/string.h"
#include "../../src/utils/array.h"

class TestArray : public Test {
public:
    Array* a1 = new Array();
    Array* a2 = new Array(5);
    String* s1 = new String("string 1");
    String* s2 = new String("string 2");
    String* s3 = new String("string 3");
    String* s4 = new String("string 4");

    ~TestArray() {
        delete a1;
        delete a2;
        delete s1;
        delete s2;
        delete s3;
        delete s4;
    }

    bool testHash() {
        assert(a1->hash() == 0);
        a1->append(s1);
        assert(a1->hash() == 1 + s1->hash());

        OK("Array::hash() -- passed.");
        return true;
    }

    bool testEquals() {
        assert(!a1->equals(a2));
        a2->append(s1);
        assert(a1->equals(a2));

        OK("Array::equals(other) -- passed.");
        return true;
    }

    bool testClone() {
        Array* a1_clone = dynamic_cast<Array *>(a1->clone());
        assert(a1_clone != nullptr);
        assert(a1->equals(a1_clone));
        a1->append(s2);
        assert(!a1->equals(a1_clone));

        delete(a1_clone->get(0));
        delete(a1_clone);

        OK("Array::clone() -- passed.");
        return true;
    }

    bool testCount() {
        assert(a1->count() == 2);
        assert(a2->count() == 1);
        a2->append(s3);
        assert(a2->count() == 2);

        OK("Array::count() -- passed.");
        return true;
    }

    bool testGet() {
        assert(a1->get(0)->equals(s1));
        assert(a1->get(1)->equals(s2));
        assert(a2->get(0)->equals(s1));
        assert(a2->get(1)->equals(s3));

        OK("Array::get(idx) -- passed.");
        return true;
    }

    bool testAppend() {
        assert(a2->count() == 2);
        a2->append(s4);
        assert(a2->count() == 3);
        assert(a2->get(2)->equals(s4));

        OK("Array::append(obj) -- passed.");
        return true;
    }

    bool testPop() {
        assert(a2->count() == 3);
        Object* popped_s4 = a2->pop(2);
        Object* popped_s1 = a2->pop(0);
        assert(popped_s4->equals(s4));
        assert(popped_s1->equals(s1));
        assert(a2->count() == 1);

        OK("Array::index(obj) -- passed.");
        return true;
    }

    bool run() {
        return testHash() 
            && testEquals() 
            && testClone() 
            && testCount() 
            && testGet() 
            && testAppend() 
            && testPop();
    }
};

int main() {
    TestArray test;
    test.testSuccess();
}