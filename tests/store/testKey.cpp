#include <assert.h>

#include "../../src/store/key.h"
#include "../test.h"

class TestKey : public Test {
public:
    Key* key = new Key("test", 5);
    Key* k = new Key("test", 5);;
    Key* not_k = new Key("hello", 4);;
    Object* obj = new Object();

    ~TestKey() {
        delete(key);
        delete(k);
        delete(not_k);
        delete(obj);
    }

    bool testEquals() {
        assert(k->equals(key));
        assert(!not_k->equals(key));
        assert(!key->equals(obj));

        OK("Key::Equals(other) - passed.");
        return true;
    }

    bool testHash() {
        size_t val = 31 * ('t' + 'e' + 's' + 't');
        assert(key->hash() == val);
        assert(key->hash() == k->hash());
        assert(key->hash() != not_k->hash());
        assert(key->hash() != obj->hash());

        OK("Key::Hash() - passed.");
        return true;
    }

    bool testClone() {
        Key* clone = dynamic_cast<Key *>(key->clone());
        Key* not_k_clone = dynamic_cast<Key *>(not_k->clone());
        assert(key->equals(clone));
        assert(!key->equals(not_k_clone));
        assert(k->idx_ == clone->idx_);

        OK("Key::Clone() - passed.");
        return true;
    }

    bool run() {
        return testEquals() && testHash() && testClone();
    }
};

int main() {
    TestKey test;
    test.testSuccess();
}