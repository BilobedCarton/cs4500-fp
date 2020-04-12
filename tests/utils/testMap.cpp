#include <assert.h>

#include "../test.h"
#include "../../src/utils/string.h"
#include "../../src/utils/map.h"

class TestMapNode : public Test {
public:
    String* k1 = new String("test key 1");
    String* k2 = new String("test key 2");
    String* k3 = new String("test key 3");
    String* v1 = new String("test val 1");
    String* v2 = new String("test val 2");
    String* v3 = new String("test val 3");
    String* v4 = new String("test val 4");
    Node* n2 = new Node(k2, v2);
    Node* n1 = new Node(k1, v1, n2);
    Node* n3 = new Node(k3, v3);

    ~TestMapNode() {
        delete(k1);
        delete(k2);
        delete(k3);
        delete(v1);
        delete(v2);
        delete(v3);
        delete(v4);
        delete(n1);
        delete(n3);
    }

    bool testCount() {
        assert(n1->count() == 2);
        assert(n2->count() == 1);

        OK("(Map) Node::count() -- passed.");
        return true;
    }

    bool testPushBack() {
        n1->pushBack(n3);
        assert(n1->count() == 3);
        assert(n2->count() == 2);
        assert(n2->next_ == n3);

        OK("(Map) Node::pushBack(n) -- passed.");
        return true;
    }

    bool testPop() {
        Node* n = n1->pop();
        assert(n == n3);
        assert(n1->count() == 2);
        assert(n2->count() == 1);

        OK("(Map) Node::pop() -- passed.");
        return true;
    }

    bool testFind() {
        assert(n1->find(k1) == n1);
        assert(n1->find(k2) == n2);
        assert(n3->find(k3) == n3);

        OK("(Map) Node::find(k) -- passed.");
        return true;
    }

    bool testGetValue() {
        assert(n1->getValue(k1)->equals(v1));
        assert(n1->getValue(k2)->equals(v2));
        assert(n3->getValue(k3)->equals(v3));

        OK("(Map) Node::getValue(k) -- passed.");
        return true;
    }

    bool testSet() {
        n1->set(k2, v4);
        n1->set(k1, v3);
        n3->set(k1, v1);
        assert(n1->getValue(k2)->equals(v4));
        assert(n1->getValue(k1)->equals(v3));
        assert(n3->count() == 2);
        assert(n3->getValue(k1)->equals(v1));

        OK("(Map) Node::set(k, v) -- passed.");
        return true;
    }

    bool run() {
        return testCount()
            && testPushBack()
            && testPop()
            && testFind()
            && testGetValue()
            && testSet();
    }
};

class TestMap : public Test {
public:
    String* k1 = new String("test key 1");
    String* k2 = new String("test key 2");
    String* k3 = new String("test key 3");
    String* k4 = new String("test key 4");
    String* k5 = new String("test key 5");
    String* k6 = new String("test key 6");
    String* v1 = new String("test val 1");
    String* v2 = new String("test val 2");
    String* v3 = new String("test val 3");
    String* v4 = new String("test val 4");
    String* v5 = new String("test val 5");
    Node* n = new Node(k3, v3);
    Map* map = new Map();

    ~TestMap() {
        delete(k1);
        delete(k2);
        delete(k3);
        delete(k4);
        delete(k5);
        delete(k6);
        delete(v1);
        delete(v2);
        delete(v3);
        delete(v4);
        delete(v5);
        delete(map);
    }

    bool testCount() {
        assert(map->count() == 0);
        map->put(k1, v1);
        map->put(k2, v2);
        assert(map->count() == 2);

        OK("Map::count() -- passed.");
        return true;
    }

    bool testGetPosition() {
        assert(map->get_position(k1) == k1->hash() % map->capacity_);
        assert(map->get_position(k3) == k3->hash() % map->capacity_);

        OK("Map::get_position(k) -- passed.");
        return true;
    }

    bool testPutNode() {
        map->put_node(n);
        assert(map->count() == 3);
        assert(map->get(k3)->equals(v3));

        OK("Map::put_node(node) -- passed.");
        return true;
    }

    bool testGrow() {
        size_t cap = map->capacity_;
        map->grow();
        assert(map->capacity_ == cap);
        map->put(k4, v4);
        map->put(k6, v1);
        assert(map->capacity_ == cap);
        map->grow();
        assert(map->capacity_ == cap * GROWTH_FACTOR);
        assert(map->get(k1) != nullptr);
        assert(map->get(k2) != nullptr);
        assert(map->get(k3) != nullptr);
        assert(map->get(k4) != nullptr);
        assert(map->get(k6) != nullptr);

        OK("Map::grow() -- passed.");
        return true;
    }

    bool testGet() {
        assert(map->get(k1)->equals(v1));
        assert(!map->get(k1)->equals(v2));
        assert(map->get(k2)->equals(v2));
        assert(map->get(k3)->equals(v3));
        assert(map->get(k4)->equals(v4));

        OK("Map::get(k) -- passed.");
        return true;
    }

    bool testPut() {
        assert(map->count() == 5);
        map->put(k5, v5);
        map->put(k4, v1);
        assert(map->count() == 6);
        assert(map->get(k5)->equals(v5));
        assert(map->get(k4)->equals(v1));

        OK("Map::put(k, v) -- passed.");
        return true;
    }

    bool run() {
        return testCount()
            && testGetPosition()
            && testPutNode()
            && testGrow()
            && testGet()
            && testPut();
    }
};

int main() {
    TestMapNode testNode;
    TestMap testMap;
    testNode.testSuccess();
    testMap.testSuccess();
}