#pragma once

#include <assert.h>
#include <string.h>

#include "../utils/object.h"

/**
 * A simple Key class used for associating and accessing objects within a KV store.
 **/
class Key : public Object {
    public:
    char* name_; // owned
    size_t idx_; // the index of the node hosting the Dataframe linked to this key

    //Key() {}

    /**
     * @brief Construct a new Key object
     * 
     * @param name - the key string used as a key
     * @param idx - the index of the node linked to this key
     */
    Key(char* name, size_t idx) {
        assert(strlen(name) > 0);
        name_ = duplicate(name);
        idx_ = idx;
    }

    /**
     * @brief Destroy the Key object
     * 
     */
    ~Key() {
        delete[](name_);
    }
    
    // inherited from object
    bool equals(Object* other) {
        Key* cast = dynamic_cast<Key *>(other);
        if(cast == nullptr) return false;
        return strcmp(name_, cast->name_) == 0;
    }

    // inherited from object
    size_t hash_me() {
        size_t h = 0;
        for (size_t i = 0; i < sizeof(name_); ++i) {
            h += 31 * name_[i]; // use a prime larger than 26
        }
        return h;
    }

    // inherited from object
    Object* clone() {
        return new Key(name_, idx_);
    }
};