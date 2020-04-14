#include <assert.h>

#include "../test.h"
#include "../../src/utils/primitivearray.h"

class TestPrimitiveArrayChunk : public Test {
public:

    PrimitiveArrayChunk<int>* largeChunk1 = new PrimitiveArrayChunk<int>(16);
    PrimitiveArrayChunk<int>* largeChunk2 = new PrimitiveArrayChunk<int>(16);
    PrimitiveArrayChunk<int>* averageChunk1 = new PrimitiveArrayChunk<int>(8);
    PrimitiveArrayChunk<int>* averageChunk2 = new PrimitiveArrayChunk<int>(8);
    PrimitiveArrayChunk<int>* smallChunk1 = new PrimitiveArrayChunk<int>(2);
    PrimitiveArrayChunk<int>* smallChunk2 = new PrimitiveArrayChunk<int>(2);

    ~TestPrimitiveArrayChunk() {
        delete(largeChunk1);
        delete(largeChunk2);
        delete(averageChunk1);
        delete(averageChunk2);
        delete(smallChunk1);
        delete(smallChunk2);
    }

    bool testPushBack() { 
        smallChunk1->push_back(1);
        smallChunk1->push_back(2); // at max capacity
        assert(smallChunk1->count() == 2 && smallChunk1->capacity_ == 2);

        bool canAddToChunk = smallChunk1->push_back(3); // will return false because chunk is full, no add occurs here
        assert(smallChunk1->count() == 2 && smallChunk1->capacity_ == 2 && !canAddToChunk);
        OK("PrimitiveArrayChunk::push_back(v) -- passed.");
        return true;
    }

    bool testPushBackMissing() {
        assert(smallChunk2->count() == 0);
        smallChunk2->push_back_missing();
        assert(smallChunk2->count() == 1);
        smallChunk2->push_back_missing();
        bool canAddToChunk = smallChunk2->push_back_missing();
        assert(smallChunk2->count() == 2 && smallChunk2->capacity_ && !canAddToChunk);

        OK("PrimitiveArrayChunk::push_back_missing() -- passed.");
        return true;
    }

    bool testIsMissing() {
        assert(smallChunk2->isMissing(0));
        assert(smallChunk2->isMissing(1));

        assert(smallChunk1->isMissing(0) == false);
        assert(smallChunk1->isMissing(1) == false);

        OK("PrimitiveArrayChunk::isMissing(idx) -- passed.");
        return true;
    }

    bool testSetMissing() {
        averageChunk1->push_back(100);
        averageChunk1->setMissing(2);

        assert(averageChunk1->isMissing(0) == false);
        assert(averageChunk1->isMissing(1));
        assert(averageChunk1->isMissing(2));

        OK("PrimitiveArrayChunk::setMissing(idx) -- passed.");
        return true;
    }

    bool testSet() {
        averageChunk2->push_back(100);
        averageChunk2->set(3, 200);

        assert(averageChunk2->isMissing(0) == false);
        assert(averageChunk2->isMissing(1) == true);
        assert(averageChunk2->isMissing(2) == true);
        assert(averageChunk2->isMissing(3) == false);

        assert(averageChunk2->get(3) == 200);
        
        OK("PrimitiveArrayChunk::set(idx, v) -- passed.");
        return true;
    }

    bool testCloneAndEquals() {
        PrimitiveArrayChunk<int>* a1clone = averageChunk1->clone();
        assert(a1clone->equals(averageChunk1));
        assert(!a1clone->equals(averageChunk2));

        largeChunk1->set(3, 50);
        largeChunk1->push_back(60);
        largeChunk1->push_back(70);
        largeChunk1->push_back(80);
        largeChunk1->push_back(90);
        largeChunk1->push_back(100);
        largeChunk1->push_back(110);
        largeChunk1->push_back(120);

        largeChunk2->set(3, 50);
        largeChunk2->push_back(60);
        largeChunk2->push_back(70);
        largeChunk2->push_back(80);
        largeChunk2->push_back(90);
        largeChunk2->push_back(100);
        largeChunk2->push_back(110);
        largeChunk2->push_back(120);

        assert(largeChunk1->equals(largeChunk2));

        OK("PrimitiveArrayChunk::clone() -- passed.");
        OK("PrimitiveArrayChunk::equals(other) -- passed.");
        return true;
    }

    bool testSerialization() {
        assert(largeChunk1->serialize()->equals(largeChunk2->serialize()));
        assert(largeChunk1->equals(PrimitiveArrayChunk<int>::deserialize(largeChunk1->serialize())));

        OK("PrimitiveArrayChunk::serialize() -- passed.");
        OK("PrimitiveArrayChunk::deserialize() -- passed.");
        return true;
    }

    bool run() {

        return testPushBack() && testPushBackMissing() && testIsMissing() && testSetMissing() && testSet() && testCloneAndEquals() && testSerialization();
    }
};

class TestPrimitiveArray : public Test {
public:

    // PrimitiveArray* pa1 = new PrimitiveArray()

    // bool testPushBackMissing() {

    // }

    bool run() {
        OK("PrimitiveArray tests not implemented.");
        return true;
    }
};

int main() {
    TestPrimitiveArray testPrimitiveArray;
    TestPrimitiveArrayChunk testPrimitiveArrayChunk;
    testPrimitiveArray.testSuccess();
    testPrimitiveArrayChunk.testSuccess();
}