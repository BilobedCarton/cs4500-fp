#pragma once

#include <assert.h>
#include <string.h>

#include "../utils/object.h"

/**
 * A simple Key class used for associating and accessing objects within a KV store.
 **/
class Key : public Object {
    public:
    char* _name; // owned
    size_t _idx; // the index of the node hosting the Dataframe linked to this key

    Key() {}

    Key(char* name, size_t idx) {
        assert(strlen(name) > 0);
        _name = new char[strlen(name) + 1];
        strcpy(_name, name);
        _name[strlen(name)] = '\0';
        _idx = idx;
    }

    ~Key() {
        delete[](name);
    }

    bool equals(Object* other) {
        Key* cast = dynamic_cast<Key *>(other);
        if(cast == nullptr) return false;
        return strcmp(_name, cast->_name) == 0;
    }

    size_t hash_me() {
        size_t h = 0;
        for (size_t i = 0; i < sizeof(_name); ++i) {
            h += 31 * _name[i]; // use a prime larger than 26
        }
        return h;
    }

    Object* clone() {
        return new Key(name, idx);
    }
};