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
    DistributedColumn<int>* dc0;
    DistributedColumn<double>* dc1;
    // DistributedColumn<String>* dc2;

    TestDistributedColumn() {
        args = new Args();
        args->num_nodes = 3;
        store0->network_->register_node(0);
        store1->start();
        store2->start();
        sleep(1);
    }

    ~TestDistributedColumn() {
        store1->running_ = false;
        store1->join();
        store2->running_ = false;
        store2->join();
        delete(store2);
        delete(store1);
        delete(store0);
        delete(dc0);
        delete(dc1);
        delete(net);
    }

    bool testConstruction() {
        dc0 = new DistributedColumn<int>(0);
        dc1 = new DistributedColumn<double>(1);
        // dc2 = new DistributedColumn<String>(2);

        dc0->set_store(store0);
        dc1->set_store(store1->store_);
        // dc2->set_store(store0);

        assert(dc0->size() == 0);
        assert(dc1->size() == 0);
        // assert(dc2->size() == 0);

        assert(dc0->idx_ == 0);
        assert(dc1->idx_ == 1);
        // assert(dc2->idx_ == 2);

        assert(dc0->keys_->count() == 0);
        assert(dc0->chunk_size_ == 4096 / sizeof(int));
        assert(dc1->chunk_size_ == 4096 / sizeof(double));
        // assert(dc2->chunk_size_ == 4096 / sizeof(String));

        assert(dc0->next_node_ == 0);
        assert(dc1->next_node_ == 0);
        // assert(dc2->next_node_ == 0);

        OK("DistributedColumn constructors -- passed.");
        return true;
    }

    bool testPushBack() {
        for (size_t i = 0; i < 4096 * 3; i++)
        {
            if(i % sizeof(int) == 0) dc0->push_back(i, nullptr);
            if(i % sizeof(double) == 0) dc1->push_back(i * 1.1, nullptr);
            // if(i % sizeof(String) == 0) {
            //     char* str = to_str<size_t>(i);
            //     String s(str);
            //     delete[](str);
            //     dc2->push_back(s, nullptr);
            // }
        }
        
        assert(dc0->size() == (4096 * 3) / sizeof(int));
        assert(dc0->last_chunk_->count() == 0);
        assert(dc0->keys_->count() == 3);
        assert(dc0->next_node_ == 0);

        assert(dc1->size() == (4096 * 3) / sizeof(double));
        assert(dc1->last_chunk_->count() == 0);
        assert(dc1->keys_->count() == 3);
        assert(dc1->next_node_ == 0);

        // assert(dc2->size() == (4096 * 3) / sizeof(String));
        // assert(dc2->last_chunk_->count() == 0);
        // assert(dc2->keys_->count() == 3);
        // assert(dc2->next_node_ == 0);

        OK("DistributedColumn::push_back(val, df) -- passed.");
        return true;
    }

    bool testGet() {
        // already supplied our columns with values so we can test those.
        for (size_t i = 0; i < 4096 * 3; i++)
        {
            if(i % sizeof(int) == 0) assert(dc0->get(i / sizeof(int)) == i);
            if(i % sizeof(double) == 0) assert(doubleAlmostEqual(dc1->get(i / sizeof(double)), i * 1.1, 3));
            // if(i % sizeof(String) == 0) {
            //     char* str = to_str<size_t>(i);
            //     String s(str);
            //     delete[](str);
            //     assert(dc2->get(i / sizeof(String)).equals(&s));
            // }
        }

        OK("DistributedColumn::get(idx) -- passed.");
        return true;
    }

    bool testSize() {
        assert(dc0->size() == (4096 * 3) / sizeof(int));
        assert(dc1->size() == (4096 * 3) / sizeof(double));
        // assert(dc2->size() == (4096 * 3) / sizeof(String));

        dc0->push_back(451, nullptr);
        dc1->push_back(0.451, nullptr);
        // String s("test");
        // dc2->push_back(s, nullptr);

        assert(dc0->size() == (4096 * 3) / sizeof(int) + 1);
        assert(dc1->size() == (4096 * 3) / sizeof(double) + 1);
        // assert(dc2->size() == (4096 * 3) / sizeof(String) + 1);

        OK("DistributedColumn::size() -- passed.");
        return true;
    }

    bool testClone() {
        DistributedColumn<int>* clone = dynamic_cast<DistributedColumn<int>*>(dc0->clone());
        assert(clone->size() == dc0->size());
        for (size_t i = 0; i < (3 * 4096) / sizeof(int) + 1; i++)
        {
            assert(dc0->get(i) == clone->get(i));
        }
        
        delete(clone);
        assert(dc0->get((4096 * 3) / sizeof(int)) == 451);
        assert(dc0->get(0) == 0);

        OK("DistributedColumn::clone() -- passed.");
        return true;
    }

    bool testEquals() {
        DistributedColumn<int>* clone = dynamic_cast<DistributedColumn<int>*>(dc0->clone());
        assert(dc0->equals(clone));
        assert(!dc1->equals(clone));
        delete(clone);

        OK("DistributedColumn::equals(other) -- passsed.");
        return true;
    }

    bool testSerialization() {
        SerialString* ss0 = dc0->serialize();
        SerialString* ss1 = dc1->serialize();

        DistributedColumn<int>* dc0_after = DistributedColumn<int>::deserialize(ss0);
        DistributedColumn<double>* dc1_after = DistributedColumn<double>::deserialize(ss1);

        assert(dc0->equals(dc0_after));
        assert(dc1->equals(dc1_after));

        delete(ss0);
        delete(ss1);
        delete(dc0_after);
        delete(dc1_after);
        
        OK("DistributedColumn serialization and deserialization -- passed.");
        return true;
    }

    bool testGetLocalChunks() {
        PrimitiveArray<int>* lc0_0 = dc0->get_local_chunks(0);
        PrimitiveArray<int>* lc0_1 = dc0->get_local_chunks(1);
        PrimitiveArray<int>* lc0_2 = dc0->get_local_chunks(2);

        assert(lc0_0->chunks_ == 2);
        assert(lc0_1->chunks_ == 1);
        assert(lc0_2->chunks_ == 1);
        delete(lc0_0);
        delete(lc0_1);
        delete(lc0_2);

        for (size_t i = 0; i < 4096 * 3 / sizeof(int); i++)
        {
            dc0->push_back(i, nullptr);
        }

        lc0_0 = dc0->get_local_chunks(0);
        lc0_1 = dc0->get_local_chunks(1);
        lc0_2 = dc0->get_local_chunks(2);

        assert(lc0_0->chunks_ == 3);
        assert(lc0_1->chunks_ == 2);
        assert(lc0_2->chunks_ == 2);
        delete(lc0_0);
        delete(lc0_1);
        delete(lc0_2);

        OK("DistributedColumn::get_local_chunks(node) -- passed.");
        return true;
    }

    bool run() {
        return testConstruction()
            && testPushBack()
            && testGet()
            && testSize()
            && testClone()
            && testEquals()
            && testSerialization()
            && testGetLocalChunks();
    }
};

int main() {
    TestDistributedColumn test;
    test.testSuccess();
}