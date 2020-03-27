#pragma once

#include <unistd.h>

#include "../utils/object.h"
#include "key.h"
#include "value.h"

#define STARTING_CAPACITY 8
#define GROWTH_FACTOR 4
#define GROWTH_THRESHOLD 0.5

class KVStore_Node : public Object {
public:
    Key* k_; // owned
    Value* v_; // owned
    KVStore_Node* next_; // owned

    KVStore_Node(Key* k, Value* v) {
        k_ = k;
        v_ = v;
        next_ = nullptr;
    }

    KVStore_Node(Key* k, Value* v, KVStore_Node* next) : KVStore_Node(k, v) {
        next_ = next;
    }

    ~KVStore_Node() {
        delete(k_);
        delete(v_);
        if(next_ != nullptr) delete(next_);
    }

    size_t count() {
        if(next_ == nullptr) return 1;
        return next_->count() + 1;
    }

    void pushBack(KVStore_Node* n) {
        if(next_ == nullptr) {
            next_ = n;
        }
        else next_->pushBack(n);
    }

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
        }
        else next_->insert(idx - 1, n);
    }

    KVStore_Node* find(Key k) {
        if(k.equals(this)) return this;
        else if(next_ == nullptr) {
            //p("Failed to find key: ").pln(k._name);
            return nullptr;
        }
        return next_->find(k);
    }

    Value* getValue(Key k) {
        KVStore_Node* node = find(k);
        if(node == nullptr) return nullptr;
        return node->v_;
    }
};

class KVStore : public Object {
public:
    KVStore_Node** nodes_;
    size_t capacity_;

    KVStore(size_t capacity) {
        capacity_ = capacity;
        size_ = 0;
        nodes_ = new KVStore_Node*[capacity_];
    }

    KVStore() : KVStore(STARTING_CAPACITY) {}

    ~KVStore() {
        for(size_t i = 0; i < size_; i++) delete(nodes_[i]);
        delete[](nodes_);
    }

    size_t count() {
        size_t count = 0;
        for (size_t i = 0; i < capacity_; i++)
        {
            if(nodes_[i] != nullptr) count += nodes_[i]->count();
        }
        return count;
    }

    size_t get_position(Key k) {
        return k.hash() % capacity_;
    }

    void put(KVStore_Node* node) {
        if(nodes_[get_position(*node->k_)] == nullptr) {
            nodes_[get_position(*node->k_)] = node;
        } else {
            nodes_[get_position(*node->k_)]->pushBack(node);
        }
    }

    void grow() {
        if(((double)count()) / capacity_ <= GROWTH_THRESHOLD) return;

        capacity_ *= GROWTH_FACTOR; 
        KVStore_Node** old = nodes_;
        nodes_ = new KVStore_Node*[capacity_];

        for (size_t i = 0; i < capacity_ / GROWTH_FACTOR; i++)
        {
            if(old[i] != nullptr) {
                KVStore_Node* node = old[i]->pop();
                while(node != nullptr) {
                    nodes_[node->]
                    node = old[i]->pop();
                    put(node);
                }
                put(old[i]);
            }
        }
        delete[](old);
        
    }

    Value* get(Key k) {
        return nodes_[get_position(k)]->getValue(k);
    }

    Value* waitAndGet(Key k) {
        while(get(k) == nullptr) { sleep(1); }
        return get(k);
    }

    KVStore* put(Key k, Value v) {
        put(new KVStore_Node(&k, &v));
        grow();
        return this;
    }
};