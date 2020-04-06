#include <assert.h>

#include "../../src/utils/string.h"
#include "../../src/utils/timer.h"
#include "../../src/utils/thread.h"
#include "../../src/store/key.h"
#include "../../src/store/value.h"
#include "../../src/store/kvstore.h"
#include "../test.h"

class TestSO : public SerializableObject {
public:
    size_t number;
    double percentage;
    char* message;

    TestSO(size_t n, double p, const char* m) {
        number = n;
        percentage = p;
        message = duplicate(m);
    }

    ~TestSO() {
        delete[](message);
    }

    SerialString* serialize() {
        size_t m_len = strlen(message);
        char* arr = new char[sizeof(size_t) + sizeof(double) + m_len];
        size_t pos = 0;

        memcpy(arr, &number, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(arr + pos, &percentage, sizeof(double));
        pos += sizeof(double);

        memcpy(arr + pos, message, m_len);
        pos += m_len;

        SerialString* ss = new SerialString(arr, pos);
        delete[](arr);

        return ss;
    }

    static TestSO* deserialize(SerialString* serialized) {
        size_t n;
        double p;

        size_t pos = 0;

        memcpy(&n, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(&p, serialized->data_ + pos, sizeof(double));
        pos += sizeof(double);

        char* m = new char[serialized->size_ - pos];
        memcpy(m, serialized->data_ + pos, serialized->size_ - pos);

        TestSO* so = new TestSO(n, p, m);
        delete[](m);
        return so;
    }
};

class TestKVStoreNode : public Test {
public:
    TestSO* so1 = new TestSO(4, 10.5, "hello");
    TestSO* so2 = new TestSO(10, 100.923, "nope");
    Key* k1 = new Key("test", 0);
    Key* k2 = new Key("other", 1);
    Key* k3 = new Key("last", 2);
    Value* v1 = new Value(so1);
    Value* v2 = new Value(so2);
    KVStore_Node* n2 = new KVStore_Node(k2, v2);
    KVStore_Node* n1 = new KVStore_Node(k1, v1, n2);
    KVStore_Node* n3 = new KVStore_Node(k3, v2);

    ~TestKVStoreNode() {
        delete(so1);
        delete(so2);
        delete(k1);
        delete(k2);
        delete(k3);
        delete(v1);
        delete(v2);
        delete(n1);
        delete(n3);
    }

    bool testCount() {
        assert(n1->count() == 2);
        assert(n2->count() == 1);
        assert(n3->count() == 1);

        OK("KVStore_Node::count() -- passed.");
        return true;
    }

    bool testPushBack() {
        assert(n1->count() == 2);
        n1->pushBack(n3);
        assert(n1->count() == 3);
        assert(n1->next_->next_ == n3);

        OK("KVStore_Node::pushBack(n) -- passed.");
        return true;
    }

    bool testPop() {
        assert(n1->count() == 3);
        KVStore_Node* n3_popped = n1->pop();
        assert(n1->count() == 2);
        KVStore_Node* n2_popped = n1->pop();
        assert(n1->count() == 1);
        assert(n1->next_ == nullptr);
        assert(n1->pop() == nullptr);
        assert(n3_popped == n3);
        assert(n2_popped == n2);
        n1->pushBack(n2);

        OK("KVStore_Node::pop() -- passed.");
        return true;
    }

    bool testFind() {
        assert(n1->find(k3) == nullptr);
        assert(n1->find(k2) == n2);
        assert(n1->find(k1) == n1);
        assert(n3->find(k3) == n3);

        OK("KVStore_Node::find(k) -- passed.");
        return true;
    }

    bool testGetValue() {
        assert(n1->getValue(k1)->serialized()->equals(v1->serialized()));
        assert(n1->getValue(k2)->serialized()->equals(v2->serialized()));
        assert(n1->getValue(k3) == nullptr);
        assert(n3->getValue(k3)->serialized()->equals(v2->serialized()));

        OK("KVStore_Node::getValue(k) -- passed.");
        return true;
    }

    bool testSet() {
        assert(n1->count() == 2);
        n1->set(k3, v1);
        assert(n1->count() == 3);
        assert(n1->find(k3) != nullptr);

        assert(n3->count() == 1);
        assert(n3->getValue(k3)->serialized()->equals(v2->serialized()));
        n3->set(k3, v1);
        assert(n3->count() == 1);
        assert(n3->getValue(k3)->serialized()->equals(v1->serialized()));

        OK("KVStore_Node::set(k, v) -- passed.");
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

class WaitAndGetThread : public Thread {
public:
    KVStore* s_;
    Key* k_;
    Value* v_;
    double time_;

    WaitAndGetThread(KVStore* s, Key* k) {
        s_ = s;
        k_ = k;
        v_ = nullptr;
    }

    void run() {
        Timer t;
        t.start();
        v_ = s_->waitAndGet(k_);
        t.stop();
        time_ = t.get_time_elapsed() / 1000; // in seconds
    }
};

class TestLocalKVStore : public Test {
public:
    PseudoNetwork* net = new PseudoNetwork(2);
    KVStore* small = new KVStore(0, net, 1);
    KVStore* reg = new KVStore(1, net);
    TestSO* so = new TestSO(9, -12.3, "testo mbesto");
    Value* v = new Value(so);

    ~TestLocalKVStore() {
        delete(small);
        delete(reg);
        delete(net);

        delete(so);
        delete(v);
    }

    bool testCount() {
        assert(small->count() == 0);
        assert(reg->count() == 0);
        Key k("test", 1);
        assert(reg->put(&k, v) == reg);
        assert(reg->count() == 1);
        Key k2("tset", 1);
        assert(reg->put(&k2, v) == reg);
        assert(reg->count() == 2);

        OK("KVStore::count() -- passed.");
        return true;
    }

    bool testGetPosition() {
        Key k("test", 0);
        assert(small->get_position(&k) == 0);
        assert(reg->get_position(&k) < reg->capacity_);

        OK("KVStore::get_position(k) -- passed.");
        return true;
    }

    bool testPutNode() {
        Key k("node", 1);
        KVStore_Node* n = new KVStore_Node(&k, v);
        assert(reg->count() == 2);
        reg->put_node(n);
        assert(reg->count() == 3);
        assert(reg->nodes_[reg->get_position(&k)] == n);

        OK("KVStore::put_node(node) -- passed.");
        return true;
    }

    bool testGrow() {
        Key k("in_small", 0);
        assert(small->capacity_ == 1);
        small->grow();
        assert(small->capacity_ == 1);
        small->put(&k, v);
        small->grow();
        assert(small->capacity_ == GROWTH_FACTOR);

        OK("KVStore::grow() -- passed.");
        return true;
    }

    bool testGet() {
        Key k("in_small", 0);
        Key kbad("test", 0);
        Key kreg("test", 1);
        assert(small->get(&k)->serialized()->equals(v->serialized()));
        //assert(small->get(&kbad) == nullptr);
        assert(reg->get(&kreg)->serialized()->equals(v->serialized()));

        OK("KVStore::get(k) -- passed.");
        return true;
    }

    bool testWaitAndGet() {
        Key k("test", 1);
        assert(reg->waitAndGet(&k)->serialized()->equals(v->serialized()));

        Key k2("test", 0);
        WaitAndGetThread* thread = new WaitAndGetThread(small, &k2);

        thread->start();

        sleep(1);
        small->put(&k2, v);

        thread->join();

        assert(thread->v_->serialized()->equals(v->serialized()));
        assert(thread->time_ >= 1.0);

        OK("KVStore::waitAndGet(k) -- passed.");
        return true;
    }

    bool testPut() {
        Key k("test2", 0);
        assert(small->get(&k) == nullptr);
        small->put(&k, v);
        assert(small->get(&k)->serialized()->equals(v->serialized()));

        TestSO so2(4, 10.5, "my fridge");
        Value v2(&so2);
        small->put(&k, &v2);
        assert(small->get(&k)->serialized()->equals(v2.serialized()));

        OK("KVStore::put(k, v) -- passed.");
        return true;
    }

    bool run() {
        return testCount()
            && testGetPosition()
            && testPutNode()
            && testGrow()
            && testGet()
            && testWaitAndGet()
            && testPut();
    }
};

int main() {
    TestKVStoreNode testNode;
    TestLocalKVStore testLocal;
    testNode.testSuccess();
    testLocal.testSuccess();
}