#pragma once

#include <unistd.h>

#include "../utils/object.h"
#include "key.h"
#include "value.h"

#define STARTING_CAPACITY 8
#define GROWTH_FACTOR 4
#define GROWTH_THRESHOLD 0.5

static size_t ID_COUNTER = 0; // used for giving new KVStores a unique id

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
                KVStore_Node* node = next_->next_;
                next_ = nullptr;
                return node;
            }
            return next_->pop();
        }
        return nullptr;

    }

    /**
     * @brief insert the given node at the given index
     * 
     * @param idx - the index in the list 
     * @param n - the node to be inserted
     */
    void insert(size_t idx, KVStore_Node* n) {
        if(idx == 0) {
            KVStore_Node holder(n->k_, n->v_);
            n->k_ = k_;
            n->v_ = v_;
            n->next_ = next_;
            k_ = holder.k_;
            v_ = holder.v_;
            next_ = n;
        }
        else if(next_ == nullptr) {
            p("Reached end of KVStore node grouping with ").p(idx).pln(" indices to go.");
            // do we want to fail outright?
        }
        else next_->insert(idx - 1, n);
    }

    /**
     * @brief finds and returns the node with the given key
     * returns nullptr if the node doesn't exit
     * 
     * @param k - the key of the node to find
     * @return KVStore_Node* - the node
     */
    KVStore_Node* find(Key k) {
        if(k.equals(this)) return this;
        else if(next_ == nullptr) {
            //p("Failed to find key: ").pln(k._name);
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
    Value* getValue(Key k) {
        KVStore_Node* node = find(k);
        if(node == nullptr) return nullptr;
        return node->v_;
    }
};

/**
 * @brief A Key Value Store where keys are a string and values are a serialized object in string form
 * 
 */
class KVStore : public Object {
public:
    size_t id_;
    KVStore_Node** nodes_; // owned, elements owned
    size_t capacity_; 

    /**
     * @brief Construct a new KVStore object with a given capacitys
     * 
     * @param capacity - the starting capacity of this store
     */
    KVStore(size_t capacity) {
        id_ = ID_COUNTER++;
        capacity_ = capacity;
        nodes_ = new KVStore_Node*[capacity_];
    }

    /**
     * @brief Construct a new KVStore object with the default starting capacity
     * 
     */
    KVStore() : KVStore(STARTING_CAPACITY) {}

    /**
     * @brief Destroy the KVStore object (destroys the nodes too)
     * 
     */
    ~KVStore() {
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
    size_t get_position(Key k) {
        return k.hash() % capacity_;
    }

    /**
     * @brief inserts a given node into its proper location in the store
     * 
     * @param node - the node to be inserted
     */
    void put(KVStore_Node* node) {
        if(nodes_[get_position(*node->k_)] == nullptr) {
            nodes_[get_position(*node->k_)] = node;
        } else {
            nodes_[get_position(*node->k_)]->pushBack(node);
        }
    }

    /**
     * @brief grows the array of nodes when the count reaches a certain threshold
     * 
     */
    void grow() {
        if(((double)count()) / capacity_ <= GROWTH_THRESHOLD) return;

        // grow the array
        capacity_ *= GROWTH_FACTOR; 
        KVStore_Node** old = nodes_;
        nodes_ = new KVStore_Node*[capacity_];

        // reinsert each node to its new proper location
        for (size_t i = 0; i < capacity_ / GROWTH_FACTOR; i++)
        {
            if(old[i] != nullptr) {
                KVStore_Node* node = old[i]->pop();
                while(node != nullptr) {
                    put(node);
                    node = old[i]->pop();
                }
                put(old[i]);
            }
        }
        delete[](old);
    }

    /**
     * @brief gets the value linked to the given key
     * 
     * @param k - the key
     * @return Value* - the linked value
     */
    Value* get(Key k) {
        return nodes_[get_position(k)]->getValue(k);
    }

    /**
     * @brief waits until a KV pair with the given key exists and then returns the value
     * 
     * @param k - the key
     * @return Value* - the linked value
     */
    Value* waitAndGet(Key k) {
        while(get(k) == nullptr) { sleep(1); } // TODO: pick actual sleeping time
        return get(k);
    }

    /**
     * @brief put the given kv pair into the store
     * 
     * @param k - the key
     * @param v - the value
     * @return KVStore* - this
     */
    KVStore* put(Key k, Value v) {
        put(new KVStore_Node(&k, &v));
        grow();
        return this;
    }
};