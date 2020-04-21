#pragma once

#include <assert.h>
#include <string.h>

#include "serial.h"
#include "string.h"

#define STARTING_CAPACITY 8

template <class T>
class PrimitiveArrayChunk : public Serializable {
public:
    T* data_; // owned
    bool* missing_; // owned
    size_t capacity_;
    size_t size_;

    PrimitiveArrayChunk(size_t capacity) {
        capacity_ = capacity;
        data_ = new T[capacity_];
        missing_ = new bool[capacity_];
        size_ = 0;
    }

    PrimitiveArrayChunk(PrimitiveArrayChunk<T>* chunk) : PrimitiveArrayChunk(chunk->capacity_) {
        for (size_t i = 0; i < chunk->size_; i++)
        {
            if(chunk->isMissing(i)) setMissing(i);
            else set(i, chunk->get(i));
        }
    }

    virtual ~PrimitiveArrayChunk() {
        delete[](data_);
        delete[](missing_);
    }

    size_t count() { return size_; } // counts missing

    bool push_back_missing() {
        if(size_ == capacity_) return false;
        missing_[size_++] = true;
        return true;
    }

    void setMissing(size_t idx) { 
        assert(idx < capacity_);
        while(size_ <= idx) {
            assert(push_back_missing());
        }
        missing_[idx] = true; 
    }

    bool isMissing(size_t idx) { 
        assert(idx < size_);
        return missing_[idx]; 
    }

    virtual bool push_back(T v) {
        if(size_ == capacity_) return false;
        data_[size_++] = v;
        missing_[size_] = false;
        return true;
    }

    virtual void set(size_t idx, T v) {
        assert(idx < capacity_);
        while(size_ <= idx) {
            assert(push_back_missing());
        }
        data_[idx] = v;
        missing_[idx] = false;
    }

    T get(size_t idx) {
        assert(isMissing(idx) != true);
        return data_[idx];
    }

    PrimitiveArrayChunk<T>* clone() { return new PrimitiveArrayChunk<T>(this); }

    virtual bool equals(PrimitiveArrayChunk<T>* other) {
        if(other == nullptr) return false;
        if(other->size_ != size_) return false;
        for (size_t i = 0; i < size_; i++)
        {
            if(isMissing(i) != other->isMissing(i)) return false;
            if(!isMissing(i)) {
                if(get(i) != other->get(i)) return false;
            }
        }
        return true;
    }

    SerialString* serialize() {
        size_t size = sizeof(size_t) + sizeof(size_t) + (sizeof(T) * capacity_) + (sizeof(bool) * capacity_);
        char* serial = new char[size];
        size_t pos = 0;
        
        // capacity
        memcpy(serial + pos, &capacity_, sizeof(capacity_));
        pos += sizeof(size_t);

        // size
        memcpy(serial + pos, &size_, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        memcpy(serial + pos, (void*)data_, sizeof(T) * capacity_);
        pos += sizeof(T) * capacity_;

        // missing
        memcpy(serial + pos, missing_, sizeof(bool) * capacity_);
        pos += sizeof(bool) * capacity_;

        SerialString* s = new SerialString(serial, pos);
        delete[](serial);

        return s;
    }

    static PrimitiveArrayChunk<T>* deserialize(SerialString* serialized) {
        size_t pos = 0;

        // capacity
        size_t cap;
        memcpy(&cap, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);
        assert(cap != 0);

        PrimitiveArrayChunk<T>* chunk = new PrimitiveArrayChunk<T>(cap);
        
        // size
        memcpy(&chunk->size_, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        memcpy(chunk->data_, serialized->data_ + pos, sizeof(T) * chunk->capacity_);
        pos += sizeof(T) * chunk->capacity_;

        // missing
        memcpy(chunk->missing_, serialized->data_ + pos, sizeof(bool) * chunk->capacity_);

        return chunk;
    }
};

template <class T>
class PrimitiveArray : public Serializable {
public:
    PrimitiveArrayChunk<T>** data_; // owned, chunks owned
    size_t chunk_size_;
    size_t chunks_;
    size_t capacity_;

    PrimitiveArray(size_t chunk_size, size_t capacity) {
        capacity_ = capacity;
        data_ = new PrimitiveArrayChunk<T>*[capacity_];
        chunks_ = 0;
        chunk_size_ = chunk_size;
    }

    PrimitiveArray(size_t chunk_size) : PrimitiveArray(chunk_size, STARTING_CAPACITY) {
        grow();
    }

    PrimitiveArray(PrimitiveArray* arr) : PrimitiveArray(arr->chunk_size_, arr->capacity_) {
        for (; chunks_ < arr->chunks_; chunks_++) {
            data_[chunks_] = new PrimitiveArrayChunk<T>(arr->data_[chunks_]);
        }
    }

    virtual ~PrimitiveArray() {
        for (size_t i = 0; i < chunks_; i++) {
            delete(data_[i]);
        }
        delete[](data_);
    }
    
    // counts missing as well
    virtual size_t count() {
        size_t count = 0;
        for (size_t i = 0; i < chunks_; i++)
        {
            count += data_[i]->count();
        }
        return count;
    }

    virtual void grow() {
        if(chunks_ == capacity_) {
            capacity_ *= 2;
            PrimitiveArrayChunk<T>** new_data = new PrimitiveArrayChunk<T>*[capacity_];
            memcpy(new_data, data_, chunks_ * sizeof(PrimitiveArrayChunk<T>*));
            delete[](data_);
            data_ = new_data;
        }
        data_[chunks_++] = new PrimitiveArrayChunk<T>(chunk_size_);
    }

    virtual void push_back_missing() {
        if(data_[chunks_ - 1]->push_back_missing()) return;
        grow();
        push_back_missing();
    }

    virtual void push_back(T v) {
        if(data_[chunks_ - 1]->push_back(v)) return;
        grow();
        push_back(v);
    }

    virtual void setMissing(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        while(chunks_ < chunk) { push_back_missing(); }
        data_[chunk]->setMissing(idx_in_chunk);
    }

    virtual bool isMissing(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        assert(chunk < chunks_);
        return data_[chunk]->isMissing(idx_in_chunk);
    }

    virtual void set(size_t idx, T v) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        while(chunks_ < chunk) { push_back_missing(); }
        data_[chunk]->set(idx_in_chunk, v);
    }

    virtual T get(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        assert(chunk < chunks_);
        return data_[chunk]->get(idx_in_chunk);
    }

    virtual PrimitiveArray<T>* clone() { return new PrimitiveArray<T>(this); }

    virtual bool equals(PrimitiveArray<T>* other) {
        if(other == nullptr) return false;
        if(other->count() != count()) return false;
        for (size_t i = 0; i < count(); i++)
        {
            if(isMissing(i) != other->isMissing(i)) return false;
            if(!isMissing(i)) {
                if(get(i) != other->get(i)) return false;
            }
        }
        return true;
    }

    virtual SerialString* serialize() {
        size_t serialized_chunk_size = sizeof(size_t) + sizeof(size_t) + (sizeof(T) * chunk_size_) + (sizeof(bool) * chunk_size_);
        char* serial = new char[sizeof(size_t) + sizeof(size_t) + (serialized_chunk_size * chunks_) + 1];
        serial[sizeof(size_t) + sizeof(size_t) + (serialized_chunk_size * chunks_)] = '\0';
        size_t pos = 0;

        // chunks
        memcpy(&serial[pos], &chunks_, sizeof(size_t));
        pos += sizeof(size_t);

        // chunk size
        memcpy(&serial[pos], &chunk_size_, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        for (size_t i = 0; i < chunks_; i++) {
            SerialString* serialized_chunk = data_[i]->serialize();
            memcpy(&serial[pos], serialized_chunk->data_, serialized_chunk->size_);
            pos+= serialized_chunk->size_;
            delete(serialized_chunk);
        }

        SerialString* s = new SerialString(serial, pos);
        delete[](serial);
        
        return s;
    }

    static PrimitiveArray<T>* deserialize(SerialString* serialized) {
        size_t chunks;
        size_t chunk_size;
        size_t pos = 0;
        
        // chunks
        memcpy(&chunks, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        // chunk size
        memcpy(&chunk_size, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        PrimitiveArray<T>* arr = new PrimitiveArray<T>(chunk_size, chunks * 2);

        // data
        size_t serialized_chunk_size = sizeof(size_t) + sizeof(size_t) + (sizeof(T) * arr->chunk_size_) + (sizeof(bool) * arr->chunk_size_);
        for (size_t i = 0; i < chunks; i++) {
            SerialString* serial = new SerialString(&serialized->data_[pos], serialized_chunk_size);
            arr->data_[arr->chunks_++] = PrimitiveArrayChunk<T>::deserialize(serial);
            pos += serialized_chunk_size;
            delete(serial);
        }

        return arr;
    }
};

class StringArrayChunk : public PrimitiveArrayChunk<String *> {
public:
    StringArrayChunk(size_t capacity) : PrimitiveArrayChunk<String *>(capacity) {}

    StringArrayChunk(StringArrayChunk* chunk) : PrimitiveArrayChunk<String *>(chunk->capacity_) {
        for (size_t i = 0; i < chunk->size_; i++)
        {
            if(chunk->isMissing(i)) setMissing(i);
            else set(i, chunk->get(i));
        }
    }

    // strings are owned
    ~StringArrayChunk() {
        for (size_t i = 0; i < size_; i++) {
            if(!isMissing(i)) delete(data_[i]);
        }
    }

    void setMissing(size_t idx) {
        if(idx < size_ && !isMissing(idx)) delete(data_[idx]);
        PrimitiveArrayChunk<String *>::setMissing(idx);
    }

    bool push_back(String* v) {
        return PrimitiveArrayChunk<String *>::push_back(v->clone());
    }

    void set(size_t idx, String* v) {
        PrimitiveArrayChunk<String *>::set(idx, v->clone());
    }

    bool equals(PrimitiveArrayChunk<String *>* other) {
        if(other == nullptr) return false;
        if(other->size_ != size_) return false;
        for (size_t i = 0; i < size_; i++)
        {
            if(isMissing(i) != other->isMissing(i)) return false;
            if(!isMissing(i)) {
                if(!get(i)->equals(other->get(i))) return false;
            }
        }
        return true;
    }

    StringArrayChunk* clone() { return new StringArrayChunk(this); }

    SerialString* serialize() {
        // data
        SerialString** serial_data = new SerialString*[size_];
        size_t data_size = 0;
        for (size_t i = 0; i < size_; i++)
        {
            char* d = "";
            size_t sz;
            if(isMissing(i)) {
                sz = 0;
            }
            else {
                Sys s;
                d = s.duplicate(get(i)->c_str());
                sz = strlen(d);
            }
            serial_data[i] = new SerialString(d, sz);
            data_size += sz + sizeof(size_t);
        }

        char* serial = new char[sizeof(size_t) + sizeof(size_t) + data_size + (sizeof(bool) * capacity_)];
        size_t pos = 0;
        
        // capacity
        memcpy(&serial[pos], &capacity_, sizeof(size_t));
        pos += sizeof(size_t);

        // size
        memcpy(&serial[pos], &size_, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        for (size_t i = 0; i < size_; i++)
        {
            memcpy(&serial[pos], &serial_data[i]->size_, sizeof(size_t));
            pos += sizeof(size_t);
            memcpy(&serial[pos], serial_data[i]->data_, serial_data[i]->size_);
            pos += serial_data[i]->size_;
            delete(serial_data[i]);
        }
        delete[](serial_data);

        // missing
        memcpy(&serial[pos], missing_, capacity_ * sizeof(bool));
        pos += capacity_ * sizeof(bool);

        SerialString* s = new SerialString(serial, pos);
        delete[](serial);

        return s;
    }

    static StringArrayChunk* deserialize(SerialString* serialized) {
        size_t pos = 0;

        // capacity
        size_t cap;
        memcpy(&cap, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);
        assert(cap != 0);

        StringArrayChunk* chunk = new StringArrayChunk(cap);
        
        // size
        memcpy(&chunk->size_, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        // data
        for (size_t i = 0; i < chunk->size_; i++)
        {
            size_t size;
            memcpy(&size, &serialized->data_[pos], sizeof(size_t));
            pos += sizeof(size_t);

            String* s = new String(&serialized->data_[pos], size);
            pos += size;

            chunk->set(i, s);
            delete(s);
        }        

        // missing
        memcpy(chunk->missing_, &serialized->data_[pos], sizeof(bool) * chunk->capacity_);
        for (size_t i = 0; i < chunk->size_; i++)
        {
            if(chunk->isMissing(i)) {
                delete(chunk->data_[i]);
                chunk->data_[i] = nullptr;
            }
        }

        return chunk;
    }
};

class StringArray {
public:
    StringArrayChunk** data_; // owned, chunks owned
    size_t chunk_size_;
    size_t chunks_;
    size_t capacity_;

    StringArray(size_t chunk_size, size_t capacity) {
        capacity_ = capacity;
        data_ = new StringArrayChunk*[capacity_];
        chunks_ = 0;
        chunk_size_ = chunk_size;
    }

    StringArray(size_t chunk_size) : StringArray(chunk_size, STARTING_CAPACITY) {
        grow();
    }

    StringArray(StringArray* arr) : StringArray(arr->chunk_size_, arr->capacity_) {
        for (; chunks_ < arr->chunks_; chunks_++) {
            data_[chunks_] = new StringArrayChunk(arr->data_[chunks_]);
        }
    }

    virtual ~StringArray() {
        for (size_t i = 0; i < chunks_; i++) {
            delete(data_[i]);
        }
        delete[](data_);
    }
    
    // counts missing as well
    virtual size_t count() {
        size_t count = 0;
        for (size_t i = 0; i < chunks_; i++)
        {
            count += data_[i]->count();
        }
        return count;
    }

    virtual void grow() {
        if(chunks_ == capacity_) {
            capacity_ *= 2;
            StringArrayChunk** new_data = new StringArrayChunk*[capacity_];
            memcpy(new_data, data_, chunks_ * sizeof(StringArrayChunk*));
            delete[](data_);
            data_ = new_data;
        }
        data_[chunks_++] = new StringArrayChunk(chunk_size_);
    }

    virtual void push_back_missing() {
        if(data_[chunks_ - 1]->push_back_missing()) return;
        grow();
        push_back_missing();
    }

    virtual void push_back(String* v) {
        if(data_[chunks_ - 1]->push_back(v)) return;
        grow();
        push_back(v);
    }

    virtual void setMissing(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        while(chunks_ < chunk) { push_back_missing(); }
        data_[chunk]->setMissing(idx_in_chunk);
    }

    virtual bool isMissing(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        assert(chunk < chunks_);
        return data_[chunk]->isMissing(idx_in_chunk);
    }

    virtual void set(size_t idx, String* v) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        while(chunks_ < chunk) { push_back_missing(); }
        data_[chunk]->set(idx_in_chunk, v);
    }

    virtual String* get(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        assert(chunk < chunks_);
        return data_[chunk]->get(idx_in_chunk);
    }

    virtual StringArray* clone() { return new StringArray(this); }

    virtual bool equals(StringArray* other) {
        if(other == nullptr) return false;
        if(other->count() != count()) return false;
        for (size_t i = 0; i < count(); i++)
        {
            if(isMissing(i) != other->isMissing(i)) return false;
            if(!isMissing(i)) {
                if(!get(i)->equals(other->get(i))) return false;
            }
        }
        return true;
    }

    virtual SerialString* serialize() {
        SerialString** chunk_serial = new SerialString*[chunks_];
        size_t chunks_size = 0;
        for (size_t i = 0; i < chunks_; i++)
        {
            chunk_serial[i] = data_[i]->serialize();
            chunks_size += chunk_serial[i]->size_;
        }
        

        char* serial = new char[sizeof(size_t) + sizeof(size_t) + chunks_size];
        size_t pos = 0;

        // chunks
        memcpy(&serial[pos], &chunks_, sizeof(size_t));
        pos += sizeof(size_t);

        // chunk size
        memcpy(&serial[pos], &chunk_size_, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        for (size_t i = 0; i < chunks_; i++)
        {
            memcpy(&serial[pos], &chunk_serial[i]->size_, sizeof(size_t));
            pos += sizeof(size_t);

            memcpy(&serial[pos], chunk_serial[i]->data_, chunk_serial[i]->size_);
            pos += chunk_serial[i]->size_;
            delete(chunk_serial[i]);
        }
        delete[](chunk_serial);
        
        SerialString* s = new SerialString(serial, pos);
        delete[](serial);
        return s;
    }

    static StringArray* deserialize(SerialString* serialized) {
        size_t chunks;
        size_t chunk_size;
        size_t pos = 0;
        
        // chunks
        memcpy(&chunks, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        // chunk size
        memcpy(&chunk_size, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        StringArray* arr = new StringArray(chunk_size, chunks * 2);

        // data
        for (size_t i = 0; i < chunks; i++) {
            size_t size;
            memcpy(&size, &serialized->data_[pos], sizeof(size_t));
            pos += sizeof(size_t);

            SerialString* s = new SerialString(&serialized->data_[pos], size);
            arr->data_[arr->chunks_++] = StringArrayChunk::deserialize(s);
            pos += s->size_;
            delete(s);
        }

        return arr;
    }
};