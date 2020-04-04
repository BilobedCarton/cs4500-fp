#pragma once

#include "object.h"

class Node {
public:
    Object* k_;
    Object* v_;
    Node * next_;

    Node(Object* k, Object* v) {
        k_ = k->clone();
        v_ = v->clone();
    }

    Node(Object* k, Object* v, Node* next) : Node(k, v) {
        next_ = next;
    }

    ~Node() {
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
    void pushBack(Node* n) {
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
    Node* pop() {
        if(next_ != nullptr) {
            if(next_->next_ == nullptr) {
                Node* node = next_;
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
    Node* find(Object* k) {
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
    Object* getValue(Object* k) {
        Node* node = find(k);
        if(node == nullptr) return nullptr;
        return node->v_;
    }

    /**
     * @brief links the given key to the given value in this node set, updating a node if the key already exists
     * 
     * @param k - the key
     * @param v - the new value
     */
    void set(Object* k, Object* v) {
        Node* node = find(k);
        if(node == nullptr) pushBack(new Node(k, v));
        else { 
            delete(node->v_);
            node->v_ = v->clone();
        }
    }
};

#define STARTING_CAPACITY 8
#define GROWTH_FACTOR 4
#define GROWTH_THRESHOLD 0.5

/**
 * @brief A map where keys are an object and values are an object
 * 
 */
class Map : public Object {
public:
    Node** nodes_; // owned, elements owned
    size_t capacity_; 

    /**
     * @brief Construct a new map object with a given capacity
     * 
     * @param capacity - the starting capacity of this store
     */
    Map(size_t capacity) {
        capacity_ = capacity;
        nodes_ = new Node*[capacity_];
        for (size_t i = 0; i < capacity_; i++)
        {
            nodes_[i] = nullptr;
        }
        
    }

    /**
     * @brief Construct a new map object with the default starting capacity
     * 
     */
    Map() : Map((size_t)STARTING_CAPACITY) {}

    /**
     * @brief Destroy the KVStore object (destroys the nodes too)
     * 
     */
    ~Map() {
        for(size_t i = 0; i < capacity_; i++) {
            if(nodes_[i] != nullptr) delete(nodes_[i]);
        }
        delete[](nodes_);
    }

    /**
     * @brief gets the number of key value pairs stored in this map
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
    size_t get_position(Object* k) {
        return k->hash() % capacity_;
    }

    /**
     * @brief inserts a given node into its proper location in the store
     * 
     * @param node - the node to be inserted
     */
    void put_node(Node* node) {
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
        if(((double)count()) / capacity_ <= GROWTH_THRESHOLD) return;

        // grow the array
        capacity_ *= GROWTH_FACTOR; 
        Node** old = nodes_;
        nodes_ = new Node*[capacity_];

        // reinsert each node to its new proper location
        for (size_t i = 0; i < capacity_ / GROWTH_FACTOR; i++)
        {
            if(old[i] != nullptr) {
                Node* node = old[i]->pop();
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
     * 
     * @param k - the key
     * @return Value* - the linked value
     */
    Object* get(Object* k) {
        if(nodes_[get_position(k)] == nullptr) return nullptr;
        return nodes_[get_position(k)]->getValue(k);
    }

    /**
     * @brief put the given kv pair into the store
     * 
     * @param k - the key
     * @param v - the value
     * @return KVStore* - this
     */
    Map* put(Object* k, Object* v) {
        grow();
        size_t pos = get_position(k);
        if(nodes_[pos] == nullptr) nodes_[pos] = new Node(k, v);
        else nodes_[pos]->set(k, v);
        return this;
    }

    // TODO: actually clone the map
    Object* clone() {
        return this;
    }
};