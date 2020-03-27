#pragma once

#include "../utils/object.h"
#include "../utils/array.h"
#include "key.h"
#include "value.h"

class KVStore : public Object {
public:
// Might be easier to not generalize Dataframes to Values?
    size_t _id;
    map<Key, Value> _kvmap;

    KVStore() {
        // assign a unique ID to the KV store from a shared global?
    }

    // Get only the local data? Seems to be the only way to accomplish the get without "waiting"
    Value* get(Key* key) {

    }

    // Grav the value related to the key from the various node clusters, return when finished
    Value* getAndWait(Key* key) {
        
    }

    void put(Key* key, Value* val) {
        
    }
};