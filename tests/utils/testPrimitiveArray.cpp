#include <assert.h>

#include "../test.h"
#include "../../src/utils/primitivearray.h"

class TestPrimitiveArrayChunk : public Test {
public:

    PrimitiveArrayChunk<int>* small = new PrimitiveArrayChunk<int>(16);
    PrimitiveArrayChunk<int>* huge1 = new PrimitiveArrayChunk<int>(2000);
    PrimitiveArrayChunk<int>* huge2 = new PrimitiveArrayChunk<int>(2000);
    StringArrayChunk* strchunk = new StringArrayChunk(16);

    ~TestPrimitiveArrayChunk() {
        delete(small);
        delete(huge1);
        delete(huge2);
        delete(strchunk);
    }

    bool testPushBack() { 
        for (size_t i = 0; i < 16; i++)
        {
            small->push_back(i);
        }
        assert(small->count() == 16 && small->capacity_ == 16);
        
        String s("test");
        strchunk->push_back(&s);
        assert(strchunk->count() == 1);

        bool canAddToChunk = small->push_back(3); // will return false because chunk is full, no add occurs here
        assert(small->count() == 16 && small->capacity_ == 16 && !canAddToChunk);

        OK("PrimitiveArrayChunk::push_back(v) -- passed.");
        return true;
    }

    bool testSet() {
        small->set(5, 100);

        assert(small->data_[5] == 100);
        assert(small->count() == 16);

        String s1("test");
        String s2("test2");
        strchunk->set(0, &s2);

        assert(s2.equals(strchunk->data_[0]));
        assert(!s1.equals(strchunk->data_[0]));
        
        OK("PrimitiveArrayChunk::set(idx, v) -- passed.");
        return true;
    }

    bool testGet() {
        for (size_t i = 0; i < 16; i++)
        {
            if(i == 5) { assert(small->get(i) == 100); continue; }
            assert(small->get(i) == i);
        }

        String s1("test");
        String s2("test2");
        strchunk->push_back(&s1);
        assert(s1.equals(strchunk->get(1)));
        assert(s2.equals(strchunk->get(0)));

        OK("PrimitiveArrayChunk::get(idx) -- passed.");
        return true;
    }

    bool testCloneAndEquals() {
        PrimitiveArrayChunk<int>* a1clone = small->clone();
        assert(a1clone->equals(small));
        delete(a1clone);

        StringArrayChunk* strclone = dynamic_cast<StringArrayChunk*>(strchunk->clone());
        assert(strclone->equals(strchunk));
        delete(strclone);

        OK("PrimitiveArrayChunk::clone() -- passed.");

        for (size_t i = 0; i < 2000; i++)
        {
            huge1->push_back(i * 100);
            huge2->push_back(i * 100);
        }

        assert(huge1->equals(huge2));

        StringArrayChunk* strchunk2 = new StringArrayChunk(16);
        String s("test");
        String s2("test2");
        strchunk2->push_back(&s2);
        strchunk2->push_back(&s);

        assert(strchunk2->equals(strchunk));
        delete(strchunk2);

        OK("PrimitiveArrayChunk::equals(other) -- passed.");
        return true;
    }

    bool testSerialization() {
        SerialString* sslc1 = huge1->serialize();

        OK("PrimitiveArrayChunk::serialize() -- passed.");

        PrimitiveArrayChunk<int>* lc1_ds = PrimitiveArrayChunk<int>::deserialize(sslc1);
        assert(huge1->equals(lc1_ds));
        assert(huge2->equals(lc1_ds));

        SerialString* sssc = strchunk->serialize();
        StringArrayChunk* strchunk_ds = StringArrayChunk::deserialize(sssc);
        assert(strchunk_ds->equals(strchunk));

        delete(sslc1);
        delete(lc1_ds);
        delete(sssc);
        delete(strchunk_ds);

        OK("PrimitiveArrayChunk::deserialize() -- passed.");
        return true;
    }

    bool run() {
        return testPushBack() 
            && testSet() 
            && testGet()
            && testCloneAndEquals() 
            && testSerialization();
    }
};


int main() {
    TestPrimitiveArrayChunk testPrimitiveArrayChunk;
    testPrimitiveArrayChunk.testSuccess();
}