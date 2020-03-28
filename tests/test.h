#include "../src/utils/helper.h"

class Test : public Sys {
public:
    bool stringEqual(char* str1, char* str2) {
        return strcmp(str1, str2) == 0;
    }

    virtual bool run() { return false; }

    void testSuccess() {
        t_true(run());
    }

    void testFail() {
        t_false(run());
    }
};