#include <assert.h>

#include "../../src/dataframe/schema.h" 
#include "../test.h"

class TestSchema : public Test {
public:
    Schema* def = new Schema();
    Schema* s = new Schema("IBS");
	String* colName = new String("float");
    Schema* copy;

    ~TestSchema() {
        delete(def);
        delete(s);
	    delete(colName);
        delete(copy);
    }

    bool run() {
        assert(def->width() == 0 && def->length() == 0);
        def->add_column('I');
        def->add_column('B');
        def->add_column('S');
        assert(def->equals(s));
        assert(def->width() == 3);

        // copy and add column
        def->add_column('F');
        copy = new Schema(*def);
        assert(def->equals(copy));
        //assert(def->col_name(3)->equals(colName));
        assert(def->ncol == 4);

        // add row
        def->add_row();
        assert(def->length() == 1);
        // assert(def->row_name(0) == nullptr);
        def->add_row();
        assert(def->length() == 2);
        // assert(def->row_name(1)->equals(colName));
        OK("Schema tests -- passed.");
        return true;
    }
};

int main() {
    TestSchema test;
    test.testSuccess();
}