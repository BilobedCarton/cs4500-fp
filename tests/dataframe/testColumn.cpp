#include <assert.h>

#include "../../src/dataframe/column.h"
#include "../test.h"

class TestColumn : public Test {
public:
    Column* c1 = new IntColumn(4, 1, 2, 3, 4);
    Column* c2 = new DoubleColumn(2, 1.5, 10.7);
    Column* c3 = new BoolColumn(1, 1);
    Column* c4 = new StringColumn();
    String* s = new String("test");
    String* newS = new String("not test");
    Column* copy = new IntColumn(5, 1, 2, 3, 4, -10);
    Object* clone;
    IntColumn* ic = c1->as_int();
    IntColumn* bad = c2->as_int();
    DoubleColumn* fc = c2->as_double();
    BoolColumn* bc = c3->as_bool();
    StringColumn* sc = c4->as_string();

    ~TestColumn() {
        delete(c1);
        delete(c2);
        delete(c3);
        delete(c4);
        delete(s);
        delete(newS);
        delete(copy);
        delete(clone);
    }

    bool testSize() {
        assert(c1->size() == 4);
        assert(c2->size() == 2);
        assert(c3->size() == 1);
        assert(c4->size() == 0);

        OK("Column::size() -- passed.");
        return true;
    }

    bool testGetType() {
        assert(c1->get_type() == 'I');
        assert(c2->get_type() == 'F');
        assert(c3->get_type() == 'B');
        assert(c4->get_type() == 'S');

        OK("Column::get_type() -- passed.");
        return true;
    }

    bool testPushBack() {
        c1->push_back(10);
        c2->push_back(100.9);
        c3->push_back(false);
        c4->push_back(s);

        assert(c1->size() == 5);
        assert(c2->size() == 3);
        assert(c3->size() == 2);
        assert(c4->size() == 1);

        OK("Column::push_back(val) -- passed.");
        return true;
    }

    bool testAsType() {
        assert(ic != nullptr);
        assert(bad == nullptr);
        assert(fc != nullptr);
        assert(bc != nullptr);
        assert(sc != nullptr);

        OK("Column::as_type() -- passed.");
        return true;
    }

    bool testGet() {
        assert(ic->get(4) == 10);
        assert(doubleAlmostEqual(fc->get(2), 100.9, 3));
        assert(bc->get(1) == false);
        assert(sc->get(0)->equals(s));

        OK("Column::get(idx) -- passed.");
        return true;
    }

    bool testSet() {
        ic->set(4, -10);
        fc->set(2, -100.9);
        bc->set(1, true);
        sc->set(0, newS);

        assert(ic->get(4) == -10);
        assert(doubleAlmostEqual(fc->get(2), -100.9, 3));
        assert(bc->get(1) == true);
        assert(!sc->get(0)->equals(s));
        assert(sc->get(0)->equals(newS));

        OK("Column::set(idx, val) -- passed.");
        return true;
    }

    bool testEquals() {
        assert(copy->data_equals(ic));
        assert(!copy->data_equals(fc));

        OK("Column::data_equals() -- passed.");
        return true;
    }

    bool testClone() {
        clone = c4->clone();
        assert(sc->data_equals(clone));

        OK("Column::clone() -- passed.");
        return true;
    }

    bool run() {
        return testSize() 
            && testGetType() 
            && testPushBack()
            && testAsType()
            && testGet()
            && testSet()
            && testEquals()
            && testClone();
    }
};

int main() {
    TestColumn test;
    test.testSuccess();
}