#include <assert.h>

#include "../test.h"
#include "../../src/utils/primitivearray.h"

class TestPrimitiveArray : public Test {
public:

    bool run() {
        OK("PrimitiveArray tests not implemented.");
        return true;
    }
};

int main() {
    TestPrimitiveArray test;
    test.testSuccess();
}