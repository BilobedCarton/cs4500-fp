#pragma once

#include <assert.h>
#include <time.h>
#include <string.h>

#include "../utils/object.h"

class Value {
public:
    char* serialized_; // owned

    Value() {}

    Value(SerializableObject so) {
        serialized_ = so.serialize();
    }

    ~Value() {
        if(serialized != nullptr) delete[](serialized_);
    }

    virtual char* serialized() { return serialized_; }

    virtual bool cachable() { return false; }

    virtual Value* clone() {
        Value* v = new Value();
        v->serialized_ = new char[strlen(serialized_) + 1];
        strcpy(v->serialized_, serialized_);
        v->serialized_[strlen(serialized_)] = '\0';
        return v;
    }

    DataFrame* asDataFrame() {
        return DataFrame::deserialize(serialized());
    }

    Column* asColumn() {
        return Column::deserialize(serialized());
    }

    ColumnChunk* asChunk() {
        return ColumnChunk::deserialize(serialized());
    }
};

class CachableValue : public Value {
public:
    char* file_; // owned
    size_t position_;
    size_t size_;
    time_t last_access_;

    CachableValue(char* file, size_t pos, size_t size) {
        file_ = new char[strlen(file) + 1];
        strcpy(file_, file);
        file_[strlen(file)] = '\0';
        position_ = pos;
        size_ = size;
    }

    CachableValue(char* file, size_t pos, SerializableObject so) : CachableValue(file, pos, sizeof(so.serialize())) {
        FILE* f = fopen(file_, "r+");
        assert(f != nullptr);
        assert(fseek(f, position_, SEEK_SET) == 0);
        assert(fwrite(so.serialize(), sizeof(char), size_ / sizeof(char), f) == size_ / sizeof(char));
        fclose(f);
    }

    CachableValue(char* file, SerializableObject so) : CachableValue(file, 0, sizeof(so.serialize())) {
        FILE* f = fopen(file_, "r+");
        assert(f != nullptr);
        assert(fseek(f, 0L, SEEK_END) == 0);
        position_ = ftell(f);
        assert(fwrite(so.serialize(), sizeof(char), size_ / sizeof(char), f) == size_ / sizeof(char));
        fclose(f);
    }

    ~CachableValue() {
        delete[](file_);
    }

    char* serialized() {
        last_access_ = time(NULL);
        if(serialized_ != nullptr) {
            return serialized_;
        }
        cache();
        return serialized_;
    }

    bool cachable() { return true; }

    Value* clone() {
        return new CachableValue(file_, pos_, size_);
    }

private:
    void cache() {
        FILE* f = fopen(file_, "r");
        assert(f != nullptr);
        assert(fseek(f, position_, SEEK_SET) == 0);
        assert(fread(serialized_, sizeof(char), size_ / sizeof(char), f) == size_);
        fclose(f);
    }

    void uncache() {
        delete[](serialized_);
    }
}