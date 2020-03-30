#include <assert.h>
#include <string.h>

#include "serial.h"

#define STARTING_CAPACITY 8

template <class T>
class PrimitiveArrayChunk : public Serializable {
    T* data_; // owned
    bool* missing_; // owned
    size_t capacity_;
    size_t size_;

    PrimitiveArrayChunk(size_t capacity) {
        capacity_ = capacity;
        data_ = new T[capacity_];
        missing_ = new T[capacity_];
        size_ = 0;

        // all values default to missing. 
        for (size_t i = 0; i < capacity_; i++) {
            missing_[i] = true;
        }
    }

    PrimitiveArrayChunk(PrimitiveArrayChunk<T>* chunk) : PrimitiveArrayChunk(chunk->capacity_) {
        for (size_t i = 0; i < capacity_; i++)
        {
            if(chunk->isMissing(i)) setMissing(i);
            else set(i, chunk->get(i));
        }
    }

    ~PrimitiveArrayChunk() {
        delete[](data_);
        delete[](missing_);
    }

    size_t count() { return size_; } // counts missing

    bool push_back_missing() {
        if(size_ == capacity_) return false;
        missing[size_++] = true;
        return true;
    }

    void setMissing(size_t idx) { 
        assert(idx < capacity_);
        while(size_ <= idx) {
            assert(push_back_missing())
        }
        missing_[idx] = true; 
    }

    bool isMissing(size_t idx) { 
        assert(idx < size_);
        return missing_[idx]; 
    }

    void push_back(T v) {
        if(size_ == capacity_) return false;
        data_[size_++] = v;
        missing_[size_] = false;
        return true;
    }

    void set(size_t idx, T v) {
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

    template <class T>
    PrimitiveArrayChunk<T>* clone() { return new PrimitiveArrayChunk<T>(this); }

    char* serialize() {
        char* serial = new char[sizeof(size_t) + sizeof(size_t) + (sizeof(T) * capacity_) + (sizeof(bool) * capacity_) + 1];
        serial[sizeof(size_t) + sizeof(size_t) + (sizeof(T) * capacity_) + (sizeof(bool) * capacity_)] = '\0';
        size_t pos = 0;
        
        // capacity
        memcpy(&serial[pos], &capacity_, sizeof(size_t));
        pos += sizeof(size_t);

        // size
        memcpy(&serial[pos], &size_, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        memcpy(&serial[pos], data_, sizeof(T) * capacity_);
        pos += sizeof(T) * capacity_;

        // missing
        memcpy(&serial[pos], missing_, sizeof(bool) * capacity_);

        return serial;
    }

    template <class T>
    static PrimitiveArrayChunk<T>* deserialize(char* serialized) {
        size_t pos = 0;

        // capacity
        size_t cap;
        memcpy(&cap, &serialized[pos], sizeof(size_t));
        pos += sizeof(size_t);
        assert(cap != 0);

        PrimitiveArrayChunk<T>* chunk = new PrimitiveArrayChunk<T>(cap);
        
        // size
        memcpy(&chunk->size_, , sizeof(size_t));
        pos += sizeof(size_t);

        // data
        memcpy(&chunk->data_, &serialized[pos], sizeof(T) * capacity_);
        pos += sizeof(T) * capacity_;

        // missing
        memcpy(&chunk->missing_, &serialized[pos], sizeof(bool) * capacity_);

        return chunk;
    }
};

template <class T>
class PrimitiveArray : public Serializable {
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

    PrimitiveArray(size_t chunk_size) : PrimitiveArray(chunk_size, STARTING_CAPACITY) {}

    PrimitiveArray(PrimitiveArray* arr) : PrimitiveArray(arr->chunk_size_, arr->capacity_) {
        for (; chunks_ < arr->chunks_; chunks_++) {
            data_[chunks_] = new PrimitiveArrayChunk<T>(arr->chunks_[i]);
        }
    }

    ~PrimitiveArray() {
        for (size_t i = 0; i < chunks_; i++) {
            delete(data_[i]);
        }
        delete[](data_);
    }
    
    // counts missing as well
    size_t count() {
        size_t count = 0;
        for (size_t i = 0; i < chunks_; i++)
        {
            count += data_[i]->count()
        }
        return count;
    }

    void grow() {
        if(chunks_ == capacity_) {
            capacity_ *= 2;
            PrimitiveArrayChunk<T>** new_data = new PrimitiveArrayChunk<T>*[capacity_];
            memcpy(new_data, data_, chunks_ * sizeof(PrimitiveArrayChunk<T>*));
            delete[](data_);
            data_ = new_data;
        }
        data_[chunks_++] = new PrimitiveArrayChunk<T>(chunk_size_);
    }

    void push_back_missing() {
        if(data_[chunks_ - 1]->push_back_missing()) return;
        grow();
        push_back_missing();
    }

    void push_back(T v) {
        if(data_[chunks_ - 1]->push_back(v)) return;
        grow();
        push_back(v);
    }

    void setMissing(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        while(chunks_ < chunk) { push_back_missing(); }
        data_[chunk]->setMissing(idx_in_chunk);
    }

    bool isMissing(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        assert(chunk < chunks_);
        return data_[chunk]->isMissing(idx_in_chunk);
    }

    void set(size_t idx, T v) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        while(chunks_ < chunk) { push_back_missing(); }
        data_[chunk]->set(idx_in_chunk, v);
    }

    T get(size_t idx) {
        size_t idx_in_chunk = idx % chunk_size_;
        size_t chunk = (idx - idx_in_chunk) / chunk_size_;
        assert(chunk < chunks_);
        return data_[chunk]->get(idx_in_chunk);
    }

    template <class T>
    PrimitiveArray<T>* clone() { return new PrimitiveArray<T>(this); }

    char* serialize() {
        size_t serialized_chunk_size = sizeof(size_t) + sizeof(size_t) + (sizeof(T) * capacity_) + (sizeof(bool) * capacity_);
        char* serial = new char[sizeof(sizeof(size_t) + sizeof(size_t) + (serialized_chunk_size * chunks_) + 1];
        serial[sizeof(sizeof(size_t) + sizeof(size_t) + (serialized_chunk_size * chunks_)] = '\0';
        size_t pos = 0;

        // chunks
        memcpy(&serial[pos], &chunks_, sizeof(size_t));
        pos += sizeof(size_t);

        // chunk size
        memcpy(&serial[pos], &chunk_size_, sizeof(size_t));
        pos += sizeof(size_t);

        // data
        for (size_t i = 0; i < chunks_; i++) {
            char* serialized_chunk = data_[i]->serialize();
            memcpy(&serial[pos], serialized_chunk, serialized_chunk_size);
            pos+= serialized_chunk_size;
            delete[](serialized_chunk);
        }
        
        return serial;
    }

    template <class T>
    static PrimitiveArray<T>* deserialize(char* serialized) {
        size_t chunks;
        size_t chunk_size;
        size_t pos = 0;
        
        // chunks
        memcpy(&chunks, &serial[pos], sizeof(size_t));
        pos += sizeof(size_t);

        // chunk size
        memcpy(&chunk_size, &serial[pos], sizeof(size_t));
        pos += sizeof(size_t);

        PrimitiveArray<T>* arr = new PrimitiveArray<T>(chunk_size, chunks * 2);

        // data
        size_t serialized_chunk_size = sizeof(size_t) + sizeof(size_t) + (sizeof(T) * capacity_) + (sizeof(bool) * capacity_);
        for (size_t i = 0; i < chunks_; i++) {
            arr->data_[arr->chunks_++] = PrimitiveArrayChunk::deserialize<T>(&serialized[pos]);
            pos += serialized_chunk_size;
        }

        return arr;
    }
};