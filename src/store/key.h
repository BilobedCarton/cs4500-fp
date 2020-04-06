#pragma once

#include <assert.h>
#include <string.h>

#include "../utils/object.h"
#include "../utils/serial.h"

/**
 * A simple Key class used for associating and accessing objects within a KV store.
 **/
class Key : public SerializableObject {
    public:
    char* name_; // owned
    size_t idx_; // the index of the node hosting the store with this key

    /**
     * @brief Construct a new Key object
     * 
     * @param name - the key string used as a key
     * @param idx - the index of the node linked to this key
     */
    Key(const char* name, size_t idx) {
        assert(strlen(name) > 0);
        name_ = duplicate(name);
        idx_ = idx;
    }

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
        return strcmp(name_, cast->name_) == 0 && idx_ == cast->idx_;
    }

    // inherited from object
    size_t hash_me() {
        size_t h = 0;
        for (size_t i = 0; i < strlen(name_); ++i) {
            h += 31 * name_[i]; // use a prime larger than 26
        }
        return h;
    }

    // inherited from object
    Object* clone() {
        return new Key(name_, idx_);
    }

    SerialString* serialize() {
        size_t name_len = strlen(name_);
        char* arr = new char[sizeof(size_t) + name_len + sizeof(size_t)];
        size_t pos = 0;

        memcpy(arr, &name_len, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(arr + pos, name_, name_len);
        pos += name_len;

        memcpy(arr + pos, &idx_, sizeof(size_t));
        pos += sizeof(size_t);

        SerialString* ss = new SerialString(arr, pos);
        delete[](arr);
        return ss;
    }

    static Key* deserialize(SerialString* serial) {
        size_t pos = 0;

        size_t name_len;
        memcpy(&name_len, serial->data_, sizeof(size_t));
        pos += sizeof(size_t);

        char* name = new char[name_len];
        memcpy(name, serial->data_ + pos, name_len);
        pos += name_len;

        size_t idx;
        memcpy(&idx, serial->data_ + pos, sizeof(size_t));

        Key* k = new Key(name, idx);
        delete[](name);

        return k;
    }
};