#include <assert.h>

#include "../../src/dataframe/schema.h"
#include "../../src/dataframe/row.h"
#include "../test.h"

class TestRow : public Test {
public:
    Schema* s = new Schema("IB");
    Row* r1 = new Row(*s);
    Row* r2 = new Row(*s);

    ~TestRow() {
        delete(s);
        delete(r1);
        delete(r2);
    }

    bool run() {
        // constructors
        assert(r1->width() == 2);
        assert(r1->col_type(0) == 'I');
        assert(r1->col_type(1) == 'B');

        // other methods
        assert(r1->data_equals(r2));
        r1->set_idx(5);
        assert(r1->get_idx() == 5);
        assert(!r1->data_equals(r2));

        OK("Row tests -- passed.");
        return true;
    }
};

int main() {
    TestRow test;
    test.testSuccess();
}