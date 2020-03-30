#include <math.h>
#include <limits>

#include "../src/utils/helper.h"

class Test : public Sys {
public:
    virtual bool run() { return false; }

    void testSuccess() {
        t_true(run());
    }

    void testFail() {
        t_false(run());
    }
};