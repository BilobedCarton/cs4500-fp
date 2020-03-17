//lang::CwC

#ifndef PART2_Map_H
#define PART2_Map_H

#endif //PART2_Map_H

#pragma once
#include "object.h"
#include <assert.h>

#include <iostream>

/**
 * @brief      This class describes a hashmap node.
 *             We use buckets represented by a linked list of pairs.
 */
class MapNode : public Object {
    public:
        Object* key_;
        Object* value_;
        MapNode* next_;

    /**
     * @brief      Constructs a new instance.
     *
     * @param      key    The key
     * @param      value  The value
     */
    MapNode(Object* key, Object* value) : Object() { 
        assert(key != nullptr || value != nullptr);
        key_ = key;
        value_ = value;
        next_ = nullptr;
    }

    /**
     * @brief      Destroys the object.
     */
    ~MapNode() {
        if (next_ != nullptr) {
            delete(next_);
        }
    }

    size_t hash() {
        return next_ == nullptr ? key_->hash() + value_->hash() : key_->hash() + value_->hash() + next_->hash();
    }

    bool equals(Object* other) {
        MapNode* cast = dynamic_cast<MapNode*>(other);
        if (cast == nullptr) return false;
        if (!key_->equals(cast->key_)) return false;
        if (!value_->equals(cast->value_)) return false;
        if (next_ != nullptr) {
            return next_->equals(cast->next_);
        } 
        return cast->next_ == nullptr;
    }

    /**
     * @brief      Sets the pair in this bucket with the given key to the given
     *             value
     *
     * @param      key    The key
     * @param      value  The new value
     *
     * @return     the new value, nullptr if no matching key is found
     */
    Object* set(Object* key, Object* value) {
        if(key->equals(key_)) {
            value_ = value;
            return value_;
        }
        if(next_ == nullptr) return nullptr;
        return next_->set(key, value);
    }

    // @brief      add another MapNode to this bucket
    //
    // @param      node  The node
    //
    void addNext(MapNode* node) {
        if (next_ != nullptr) {
            next_->addNext(node);
        } else {
           next_ = node; 
        }
    }

    /**
     * @brief      Starts the recursive remove.
     *
     * @param      key     The key
     * @param      origin  The origin
     *
     * @return     value of the removed pair (or nullptr if not found)
     */
    Object* remove_start(Object* key, MapNode** origin) {
        if (key_->equals(key)) {
            *origin = next_;
            Object* val = value_;
            delete(this);
            return val;
        } 

        if (next_ == nullptr) {
            return nullptr;
        }

        return next_->remove(key, this);
    }

    /**
     * @brief      removes the pair with the given key
     *
     * @param      key   The key
     * @param      prev  The previous node
     *
     * @return     the value of the removed pair (or nullptr if no pair could be found)
     */
    Object* remove(Object* key, MapNode* prev) {
        if (key_->equals(key)) {
            prev->next_ = next_;
            Object* val = value_;
            delete(this);

            return val;
        } 

        if (next_ == nullptr) {
            return nullptr;
        }

        return next_->remove(key, this);
    }

    // @brief      Return the number of values in this key bucket
    //
    // @return     the number of values in this bucket
    //
    size_t count() {
        if (next_ == nullptr) {
            return 1;
        }
        return 1 + next_->count(); // recursive call
    }

    /**
     * @brief      Gets the key at the given index in this bucket.
     *
     * @param[in]  idx   The index
     *
     * @return     The key.
     */
    Object* getKey(size_t idx) {
        if (idx == 0) return key_;
        if (next_ == nullptr) return nullptr;
        return next_->getKey(idx - 1);
    }

    /**
     * @brief      Gets the value at the given index in this bucket.
     *
     * @param[in]  idx   The index
     *
     * @return     The value.
     */
    Object* getValue(size_t idx) {
        if (idx == 0) return value_;
        if (next_ == nullptr) return nullptr;
        return next_->getValue(idx - 1);
    }

    /**
     * @brief      Gets the value from the pair with the given key
     *
     * @param      key   The key
     *
     * @return     The value.
     */
    Object* getValue(Object* key) {
        if (key->equals(key_)) {
            return value_;
        }
        if (next_ == nullptr) {
            return nullptr;
        }

        return next_->getValue(key);
    }
};

class Map : public Object {
public:
    MapNode** keyValues;
    size_t size_;
    size_t capacity_;

    /**
     * Default constructor that constructs an empty Map with
     * the default initial capacity 16
    */
    Map() {
        size_ = 0;
        capacity_ = 16;
        keyValues = new MapNode*[capacity_];
        for (size_t i = 0; i < 16; ++i)
        {
            keyValues[i] = nullptr;
        }
    }

    /**
     * Destructor that delete Map object
     */
    ~Map() {
        for (size_t i = 0; i < capacity_; i++) {
            if (keyValues[i] != nullptr) {
                delete(keyValues[i]);
            }
        }

        delete[](keyValues);
    }

public:
    /**
     * Returns the number of key-value pairs in this map.
     */
    int get_size() {
        return size_;
    }

    /**
     * increase or decrease the capacity of the map
     */
    void resize() {
        size_t size = size_;
        Object** keys = key_set();
        Object** vals = values();
        for (size_t i = 0; i < capacity_; ++i)
        {
            delete(keyValues[i]);
        }
        delete[](keyValues);
        if (size_ / capacity_ > 16) {
            // Grow
            capacity_ *= 4;
        } else if (capacity_ / size_ > 16) {
            // Shrink
            capacity_ /= 4;
        }
        keyValues = new MapNode*[capacity_];
        size_ = 0;
        for (size_t i = 0; i < size; ++i)
        {
            put(keys[i], vals[i]);
        }
    }

    /**
     * Put the given key value pair into the map
     * If the key is already associated with a value, the new value should overwrite the previous one
     * @return  val
     */
    Object* put(Object* key, Object* val) {
        size_t keyIdx = key->hash() % capacity_;
        if (!contains_key(key)) size_++;
        else {
            return keyValues[keyIdx]->set(key, val);
        }
        if (keyValues[keyIdx] == nullptr) {
            keyValues[keyIdx] = new MapNode(key, val);
            return val;
        }
        keyValues[keyIdx]->addNext(new MapNode(key, val));
        return val;
    }


    /**
     * Returns the value of which the specified key is mapped to, or nullptr if this map does not contain the given key
     * @param key   the key whose associated value is to be returned
     * @return  the value mapped to the given key, or nullptr if the key is not found
     */
    Object* get(Object* key) {
        size_t keyIdx = key->hash() % capacity_;
        if (keyValues[keyIdx] == nullptr) {
            return nullptr;
        }
        return keyValues[keyIdx]->getValue(key);
    }


    /**
     * Returns true if this map contains the given key
     * @param key   The key whose presence in this map is to be tested
     * @return  true if this map contains a mapping for the specified key, otherwise false
     */
    bool contains_key(Object* key) {
        return get(key) != nullptr;
    }


    /**
     * Removes the mapping for the specified key from this map if present.
     * @param key
     * @return   value associated with the key, or nullptr if the key is not found
     */
    Object* remove(Object* key) {
        size_t keyIdx = key->hash() % capacity_;
        if (keyValues[keyIdx] == nullptr) {
            return nullptr;
        }
        Object* val = keyValues[keyIdx]->remove_start(key, &keyValues[keyIdx]);
        if(val == nullptr) return nullptr;
        size_--;
        return val;
    }


    /**
     * @return  a list of the keys contained in this map
     */
    Object** key_set() {
        Object** set = new Object*[size_];
        size_t curIdx = size_ - 1;
        for (size_t i = 0; i < capacity_; i++) {
            if (keyValues[i] != nullptr) {
                for (size_t j = 0; j < keyValues[i]->count(); j++) {
                    set[curIdx--] = keyValues[i]->getKey(j);
                }
            }
        }

        return set;
    }


    /**
     * @return  a list of values contained in this map
     */
    Object** values() {
        Object** set = new Object*[size_];
        size_t curIdx = 0;
        for (size_t i = 0; i < capacity_; i++) {
            if (keyValues[i] != nullptr) {
                for (size_t j = 0; j < keyValues[i]->count(); j++) {
                    set[curIdx++] = keyValues[i]->getValue(j);
                }
            }
        }

        return set;
    }

    size_t hash() {
        size_t hashVal = 0;
        for (size_t i = 0; i < capacity_; i++) {
            if (keyValues[i] != nullptr) {
                hashVal += keyValues[i]->hash();
            }
        }
        return hashVal;
    }

    bool equals(Object* object) {
        Map* cast = dynamic_cast<Map*>(object);
        if (cast == nullptr) return false;

        for (size_t i = 0; i < capacity_; i++) {
            if (keyValues[i] == nullptr) {
                if (cast->keyValues[i] != nullptr) {
                    return false;
                }
            } else {
                if (!keyValues[i]->equals(cast->keyValues[i])) return false;
            }
        }

        return true;
    }
};