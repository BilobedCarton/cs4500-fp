#pragma once

#include <assert.h>
#include <time.h>
#include <string.h>

//#include "../utils/object.h"
#include "../utils/serial.h"
#include "../utils/helper.h"

/**
 * @brief a value used in a KVStore
 * 
 */
class Value : public Sys {
public:
    SerialString* serialized_; // owned

    /**
     * @brief Construct a new Value object
     * Default constructor for use by child classes
     */
    Value() {
        serialized_ = nullptr;
    }

    /**
     * @brief Construct a new Value object
     * 
     * @param so the serializable object stored in this value
     */
    Value(SerializableObject* so) {
        serialized_ = so->serialize();
    }

    /**
     * @brief Destroy the Value object
     * Checks if serialized exists because child classes may have deleted it (caching)
     */
    virtual ~Value() {
        if(serialized_ != nullptr) delete(serialized_);
    }

    /**
     * @brief get the serial string stored in this value
     * 
     * @return char* - the serial string
     */
    virtual SerialString* serialized() { return serialized_; }

    /**
     * @brief determines if this value is cachable and can save memory usage
     * 
     * @return true - if this is cachable
     * @return false - if this is not and is always stored in memory
     */
    virtual bool cachable() { return false; }

    /**
     * @brief clones this value
     * 
     * @return Value* - the clone of this value
     */
    virtual Value* clone() {
        Value* v = new Value();
        v->serialized_ = serialized()->clone();
        return v;
    }
};

/**
 * @brief a value that is stored on file and can be cached in memory
 * 
 */
class CachableValue : public Value {
public:
    char* file_; // owned
    size_t position_;
    size_t size_;
    time_t last_access_ = 0;

    /**
     * @brief Construct a new Cachable Value object
     * 
     * @param file - the file the value is stored in
     * @param pos - the position in the file of the stored value
     * @param size - the size of the stored value
     */
    CachableValue(char* file, size_t pos, size_t size) {
        file_ = new char[strlen(file) + 1];
        strcpy(file_, file);
        file_[strlen(file)] = '\0';
        position_ = pos;
        size_ = size;
    }

    /**
     * @brief Construct a new Cachable Value object
     * 
     * @param file - the file the value is stored in
     * @param pos - the position in the file of the stored value
     * @param so - the serializable object represented by this value
     */
    CachableValue(char* file, size_t pos, SerializableObject* so) : CachableValue(file, pos, sizeof(so->serialize())) {
        FILE* f = fopen(file_, "r+");
        assert(f != nullptr);
        assert(fseek(f, position_, SEEK_SET) == 0);
        assert(fwrite(so->serialize(), sizeof(char), size_ / sizeof(char), f) == size_ / sizeof(char));
        fclose(f);
    }

    /**
     * @brief Construct a new Cachable Value object
     * 
     * @param file - the file the value is stored in
     * @param so - the serializable object represented by this value
     */
    CachableValue(char* file, SerializableObject* so) {
        file_ = new char[strlen(file) + 1];
        strcpy(file_, file);
        file_[strlen(file)] = '\0';
        SerialString* serialized = so->serialize();
        size_ = serialized->size_;

        FILE* f = fopen(file_, "r+");
        assert(f != nullptr);
        assert(fseek(f, 0L, SEEK_END) == 0);
        position_ = ftell(f);
        assert(fwrite(serialized, sizeof(char), size_ / sizeof(char), f) == size_ / sizeof(char));
        fclose(f);
    }

    /**
     * @brief Destroy the Cachable Value object
     * 
     */
    ~CachableValue() {
        delete[](file_);
    }

    /**
     * @brief caches the value in memory
     * 
     */
    void cache() {
        char * serialized = new char[size_ / sizeof(char)];

        FILE* f = fopen(file_, "r");
        assert(f != nullptr);
        assert(fseek(f, position_, SEEK_SET) == 0);
        assert(fread(serialized_, sizeof(char), size_ / sizeof(char), f) == size_);
        fclose(f);

        serialized_ = new SerialString(serialized, size_);
        delete[](serialized);
    }

    /**
     * @brief deletes the cached data and reverts last access time
     * 
     */
    void uncache() {
        delete[](serialized_);
        serialized_ = nullptr;
        last_access_ = 0;
    }

    /**
     * @brief set access time and cache if necessary
     * 
     * @return char* 
     */
    SerialString* serialized() {
        last_access_ = time(NULL);
        if(serialized_ == nullptr) {
            cache();
        }
        return Value::serialized();
    }

    // inherited from parent class
    bool cachable() { return true; }

    // inherited
    Value* clone() {
        return new CachableValue(file_, position_, size_);
    }
};