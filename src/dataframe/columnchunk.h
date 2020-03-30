#include <assert.h>
#include <stdlib.h>

#include "../utils/object.h"
#include "../utils/array.h"
#include "../utils/string.h"

static size_t ID_COUNTER = 0;

/*
 * A ColumnChunk holds a portion of data belonging to a larger singular Column entity.
 * Chunk data is stored serialized for easy shipping around the cluster, only being deserialized for resassembly by another entity.
 */
class ColumnChunk : public Object {
public: 
    size_t _id; // identifier for this chunk
    char* _data; // serialized version of this chunk's data
    ColumnChunk* _next; // some sort of reference to the next piece of data (null if final chunk?)

    ColumnChunk() {
        _id = ID_COUNTER++;
        _data = data;
    }

    // Set a reference to the next piece of chunk data, if any.
    void setNext(ColumnChunk* next) {
        assert(next != nullptr);

        _next = next; // make a copy?
    }

    // Grab data from this chunk and any which follow it recursively
    char* retrieveData() {
        // more column data exists in a different chunk
        if (_next != nullptr) {
            // pull the data 
            _next->retrieveData(); // append to a StrBuf?
        }

        // return the StrBuf
    }

};