#pragma once

//#include <unistd.h>

#include "../utils/object.h"
#include "key.h"
#include "value.h"
#include "message.h"
#include "network.h"

#define STARTING_CAPACITY 8
#define GROWTH_FACTOR 4
#define GROWTH_THRESHOLD 0.5

/**
 * @brief A node stored in the KVStore that allows for collision handling
 * 
 */
class KVStore_Node : public Object {
public:
    Key* k_; // owned
    Value* v_; // owned
    KVStore_Node* next_; // owned

    /**
     * @brief Construct a new kvstore node object
     * 
     * @param k - key
     * @param v - value
     */
    KVStore_Node(Key* k, Value* v) {
        k_ = dynamic_cast<Key *>(k->clone());
        v_ = v->clone();
        next_ = nullptr;
    }

    /**
     * @brief Construct a new kvstore node object
     * 
     * @param k - key
     * @param v - value
     * @param next - next node in the cycle
     */
    KVStore_Node(Key* k, Value* v, KVStore_Node* next) : KVStore_Node(k, v) {
        next_ = next;
    }

    /**
     * @brief Destroy the kvstore node object
     * If this has a linked node in succession, we delete that too
     */
    ~KVStore_Node() {
        delete(k_);
        delete(v_);
        if(next_ != nullptr) delete(next_);
    }

    /**
     * @brief get the number of nodes (inclusive) in this linked set
     * 
     * @return size_t - the number of nodes
     */
    size_t count() {
        if(next_ == nullptr) return 1;
        return next_->count() + 1;
    }

    /**
     * @brief append the given node to the end of this set
     * 
     * @param n - the node to be appended
     */
    void pushBack(KVStore_Node* n) {
        if(next_ == nullptr) {
            next_ = n;
        }
        else next_->pushBack(n);
    }

    /**
     * @brief pop the last node of this set
     * 
     * @return KVStore_Node* - the popped node
     */
    KVStore_Node* pop() {
        if(next_ != nullptr) {
            if(next_->next_ == nullptr) {
                KVStore_Node* node = next_;
                next_ = nullptr;
                return node;
            }
            return next_->pop();
        }
        return nullptr;
    }

    /**
     * @brief finds and returns the node with the given key
     * returns nullptr if the node doesn't exit
     * 
     * @param k - the key of the node to find
     * @return KVStore_Node* - the node
     */
    KVStore_Node* find(Key* k) {
        if(k->equals(k_)) return this;
        else if(next_ == nullptr) {
            return nullptr;
        }
        return next_->find(k);
    }

    /**
     * @brief Get the Value object of the node with the given key
     * returns nullptr if the key can't be found
     * 
     * @param k - the key mapped to the value
     * @return Value* - the value mapped to the key
     */
    Value* getValue(Key* k) {
        KVStore_Node* node = find(k);
        if(node == nullptr) return nullptr;
        return node->v_;
    }

    /**
     * @brief links the given key to the given value in this node set, updating a node if the key already exists
     * 
     * @param k - the key
     * @param v - the new value
     */
    void set(Key* k, Value* v) {
        KVStore_Node* node = find(k);
        if(node == nullptr) pushBack(new KVStore_Node(k, v));
        else { 
            delete(node->v_);
            node->v_ = v->clone();
        }
    }

    /**
     * @brief removes the node with the given k
     * 
     * @param k - 
     */
    void remove(Key* k) {
        if(next_ == nullptr) return;
        else if(next_->k_->equals(k)) {
            KVStore_Node* node = next_;
            next_ = node->next_;
            delete(node);
        }
        else {
            next_->remove(k);
        }
    }
};

class KVStore; // forward dec

class NetworkListener : public Thread {
public:
    size_t fail_count_;
    Lock prod_;
    Lock cons_;
    KVStore* store_; // external
    Status* s_; // owned

    NetworkListener(KVStore* store) {
        fail_count_ = 0;
        store_ = store;
        s_ = nullptr;
    }

    ~NetworkListener() { if(s_ != nullptr) delete(s_); }

    Status* await_status() {
        //prod_.notify_all(); // let producer know we need a status
        if(s_ == nullptr) { prod_.notify_all(); cons_.wait(); } // wait until s_ available
        fail_count_ = 0;
        Status* s = s_;
        s_ = nullptr;
        cons_.unlock();
        prod_.notify_all();
        return s;
    }

    void handleGet(Get* g);

    void run();
};

/**
 * @brief A Key Value Store where keys are a string and values are a serialized object in string form
 * 
 */
class KVStore : public Object {
public:
    size_t idx_;
    Lock prod_;
    Lock cons_;
    NetworkIfc* network_; // unowned
    NetworkListener listener_;
    KVStore_Node** nodes_; // owned, elements owned
    size_t capacity_; 

    /**
     * @brief Construct a new KVStore object with a given capacitys
     * 
     * @param capacity - the starting capacity of this store
     */
    KVStore(size_t idx, NetworkIfc* network, size_t capacity) : listener_(this) {
        idx_ = idx;
        network_ = network;

        capacity_ = capacity;
        nodes_ = new KVStore_Node*[capacity_];
        for (size_t i = 0; i < capacity_; i++)
        {
            nodes_[i] = nullptr;
        }

        listener_.start();
    }

    /**
     * @brief Construct a new KVStore object with the default starting capacity
     * 
     */
    KVStore(size_t idx, NetworkIfc* network) : KVStore(idx, network, STARTING_CAPACITY) {}

    /**
     * @brief Destroy the KVStore object (destroys the nodes too)
     * 
     */
    ~KVStore() {
        listener_.store_ = nullptr;
        listener_.join();
        for(size_t i = 0; i < capacity_; i++) {
            if(nodes_[i] != nullptr) delete(nodes_[i]);
        }
        delete[](nodes_);
    }

    /**
     * @brief gets the number of key value pairs stored in this KVStore
     * 
     * @return size_t - the number of kv pairs
     */
    size_t count() {
        size_t count = 0;
        for (size_t i = 0; i < capacity_; i++)
        {
            if(nodes_[i] != nullptr) count += nodes_[i]->count();
        }
        return count;
    }

    /**
     * @brief Get the index of the given key
     * 
     * @param k - the key
     * @return size_t - the index
     */
    size_t get_position(Key* k) {
        return k->hash() % capacity_;
    }

    /**
     * @brief inserts a given node into its proper location in the store
     * 
     * @param node - the node to be inserted
     */
    void put_node(KVStore_Node* node) {
        if(nodes_[get_position(node->k_)] == nullptr) {
            nodes_[get_position(node->k_)] = node;
        } else {
            nodes_[get_position(node->k_)]->pushBack(node);
        }
    }

    /**
     * @brief grows the array of nodes when the count reaches a certain threshold
     * 
     */
    void grow() {
        // do we need to grow?
        if(((double)count()) / capacity_ <= GROWTH_THRESHOLD) return;

        // yes, grow the array
        capacity_ *= GROWTH_FACTOR; 
        KVStore_Node** old = nodes_;
        nodes_ = new KVStore_Node*[capacity_];
        for (size_t i = 0; i < capacity_; i++)
        {
            nodes_[i] = nullptr;
        }

        // reinsert each node to its new proper location
        for (size_t i = 0; i < capacity_ / GROWTH_FACTOR; i++)
        {
            if(old[i] != nullptr) {
                KVStore_Node* node = old[i]->pop();
                while(node != nullptr) {
                    put_node(node);
                    node = old[i]->pop();
                }
                put_node(old[i]);
            }
        }
        delete[](old);
    }

    /**
     * @brief gets the value linked to the given key
     * Force wait and get if provided with external key (key in another node)
     * 
     * @param k - the key
     * @return Value* - the linked value
     */
    Value* get(Key* k) {
        if(k->idx_ != idx_) return waitAndGet(k);

        Value* v;
        prod_.lock();
        if(nodes_[get_position(k)] == nullptr) v = nullptr;
        else v = nodes_[get_position(k)]->getValue(k);
        if (v != nullptr) {
            v = v->clone();
        }
        prod_.unlock();

        return v;
    }

    /**
     * @brief waits until a KV pair with the given key exists and then returns the value
     * 
     * @param k - the key
     * @return Value* - the linked value
     */
    Value* waitAndGet(Key* k) {
        Value* v;
        if(k->idx_ == idx_) {
            v = get(k);
            while(v == nullptr) { cons_.unlock(); cons_.wait(); v = get(k); } // TODO: pick actual sleeping time
            cons_.unlock();
        }
        else { // send a request on the network
            Get* g = new Get(k);
            g->sender_ = idx_;
            network_->send_message(g);
            Status* s = listener_.await_status();
            v = s->v_->clone();
            delete(s);
        }
        return v;
    }

    /**
     * @brief put the given kv pair into the store
     * 
     * @param k - the key
     * @param v - the value
     * @return KVStore* - this
     */
    KVStore* put(Key* k, Value* v) {
        if(k->idx_ != idx_) {
            Put* p = new Put(k, v);
            p->sender_ = idx_;
            network_->send_message(p);
        }
        else {
            prod_.lock();
            grow();
            size_t pos = get_position(k);
            if(nodes_[pos] == nullptr) nodes_[pos] = new KVStore_Node(k, v);
            else nodes_[pos]->set(k, v);
            prod_.unlock();
            cons_.notify_all();
        }
        return this;
    }

    /**
     * @brief removes the kv pair with the given key
     * should only be used locally
     * 
     * @param k - the key
     */
    void remove(Key* k) {
        assert(k->idx_ == idx_);
        size_t pos = get_position(k);
        if(nodes_[pos] == nullptr) return;
        if(nodes_[pos]->k_->equals(k)) {
            KVStore_Node* node = nodes_[pos];
            nodes_[pos] = node->next_;
            delete(node);
        }
        else {
            nodes_[pos]->remove(k);
        }
    }
};

void NetworkListener::handleGet(Get* g)  {
    Value* v = store_->get(g->k_);
    Message* m;
    if(v == nullptr) m = new Fail(g->k_);
    else m = new Status(g->sender_, v);
    m->sender_ = store_->idx_;
    m->target_ = g->sender_;
    store_->network_->send_message(m);
    delete(g);
}

void NetworkListener::run() {
    if(dynamic_cast<PseudoNetwork *>(store_->network_) != nullptr) store_->network_->register_node(store_->idx_);
    while(store_ != nullptr) { // go forever
        Message* send;
        Message* m = store_->network_->receive_message();
        Get* g = dynamic_cast<Get *>(m);
        Put* p = dynamic_cast<Put *>(m);
        Fail* f = dynamic_cast<Fail *>(m);
        if(m == nullptr) continue;
        switch(m->type_) {
            case MsgType::Register:
                break; // ignore
            case MsgType::Get:
                handleGet(g);
                break;
            case MsgType::Put:
                store_->put(p->k_, p->v_);
                delete(p);
                break;
            case MsgType::Status:
                if(s_ != nullptr) { cons_.notify_all(); prod_.wait(); }// Wait until s_ consumed
                s_ = dynamic_cast<Status *>(m);
                prod_.unlock();
                cons_.notify_all(); // s_ available
                break;
            case MsgType::Directory:
                break; // ignore
            case MsgType::Fail:
                // wait, and then resend the get
                fail_count_ += 1;
                sleep(fail_count_ * 1000);
                send = new Get(f->k_);
                send->sender_ = store_->idx_;
                store_->network_->send_message(send);
                delete(f);
                break;
            default:
                assert(false);
                break;
        }
    }
}