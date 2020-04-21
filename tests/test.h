#include <math.h>
#include <limits>

#include "../src/utils/helper.h"
#include "../src/utils/logger.h"

class Test : public Sys {
public:
    Test(bool suppress_logging) {
        SUPPRESS_LOGGING = suppress_logging;
    }

    Test() : Test(true) { }

    virtual bool run() { return false; }

    void testSuccess() {
        t_true(run());
    }

    void testFail() {
        t_false(run());
    }
};