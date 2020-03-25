#pragma once

#include <assert.h>
#include <string.h>

#include "../utils/object.h"

/**
 * A simple Key class used for associating and accessing objects within a KV store.
 **/
class Key : public Object {
    public:
    char* _name;
    size_t _idx; // the index of the node hosting the Dataframe linked to this key

    Key(char* name, size_t idx) {
        assert(strlen(name) > 0);
        _name = name;
        _idx = idx;
    }
};