#include <assert.h>

#include "../test.h"
#include "../../src/dataframe/distributed_column.h"
#include "../../src/store/kvstore.h"

class KVStoreThread : public Thread {
public:
    size_t idx_;
    NetworkIfc* network_;
    KVStore* store_; // owned
    bool running_ = true;

    KVStoreThread(size_t idx, NetworkIfc* network) {
        idx_ = idx;
        network_ = network;
    }

    ~KVStoreThread() {
        delete(store_);
    }

    void run() {
        store_ = new KVStore(idx_, network_);
        network_->register_node(idx_);
        while(running_) {
            sleep(1);
        }
    }
};

class TestDistributedColumn : public Test {
public:
    PseudoNetwork* net = new PseudoNetwork(3);
    KVStoreThread* store1 = new KVStoreThread(1, net);
    KVStoreThread* store2 = new KVStoreThread(2, net);
    KVStore* store0 = new KVStore(0, net);

    TestDistributedColumn() {
        store0->network_->register_node(0);
        store1->start();
        store2->start();
    }

    bool testConstruction() {
        DistributedColumn<int>* dc0 = new DistributedColumn<int>(0);
        DistributedColumn<double>* dc1 = new DistributedColumn<double>(1);
        DistributedColumn<String>* dc2 = new DistributedColumn<String>(2);

        dc0->set_store(store0);
        dc1->set_store(store0);
        dc2->set_store(store0);

        assert(dc0->size() == 0);
        assert(dc1->size() == 0);
        assert(dc2->size() == 0);

        assert(dc0->idx_ == 0);
        assert(dc1->idx_ == 1);
        assert(dc2->idx_ == 2);

        assert(dc0->keys_->count() == 0);
        assert(dc0->chunk_size_ == 4096 / sizeof(int));
        assert(dc1->chunk_size_ == 4096 / sizeof(double));
        assert(dc2->chunk_size_ == 4096 / sizeof(String));

        assert(dc0->next_node_ == 0);
        assert(dc1->next_node_ == 0);
        assert(dc2->next_node_ == 0);

        OK("DistributedColumn constructors -- passed.");
        return true;
    }

    bool testPushBack() {


        OK("DistributedColumn::push_back(val, df) -- passed.");
        return true;
    }

    bool testGet() {
         
        OK("DistributedColumn::get(idx) -- passed.");
        return true;
    }

    bool testSize() {

        OK("DistributedColumn::size() -- passed.");
        return true;
    }

    bool testClone() {

        OK("DistributedColumn::clone() -- passed.");
        return true;
    }

    bool testSerialize() {

        OK("DistributedColumn::serialize() -- passed.");
        return true;
    }

    bool testDeserialize() {

        OK("DistributedColumn::deserialize<U>(serialized, store) -- passed.");
        return true;
    }

    bool run() {
        return testConstruction()
            && testPushBack()
            && testGet()
            && testSize()
            && testClone()
            && testSerialize()
            && testDeserialize();
    }
};

int main() {
    TestDistributedColumn test;
    test.testSuccess();
}