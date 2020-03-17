// lang::CwC
#pragma once

#include <cstddef>
#include <string.h>
#include <assert.h>

#include "object.h"
#include "string.h"
#include "serial.h"
#include "helper.h"

// String extends Object so Array supports String

// Resizeable array with amortized appending to end of list and constant 
// time retrieval
class Array : public Object {
   public:
        size_t size_;
        size_t capacity_;
        Object** list_;

    // Empty constructor
    Array() {
        size_ = 0;
        capacity_ = 4;
        list_ = new Object*[capacity_];
    }

    // capacity given constructor
    Array(size_t capacity) {
        size_ = 0;
        capacity_ = capacity;
        list_ = new Object*[capacity_];
    }

    // Deep copy of other array
    Array(const Array* arr) {
        size_ = arr->size_;
        capacity_ = arr->capacity_;
        list_ = new Object*[capacity_];
        for (int i = 0; i < size_; ++i)
        {
            list_[i] = arr->list_[i] == nullptr ? nullptr : arr->list_[i]->clone();
        }
    }

    // Clear contents of this before this is freed
    ~Array() {
        delete[](list_);
    }

    // Returns hash code for this
    size_t hash() {
        if(size_ == 0) { return 0; }
        size_t h = size_;
        for (int i = 0; i < size_; ++i)
        {
            h += list_[i]->hash();
        }
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(Object* obj) {
        Array* arr = dynamic_cast<Array *>(obj);
        if(arr == NULL) return false;
        if(size_ != arr->size_) return false;
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] == nullptr && arr->list_[i] != nullptr) return false;
            if(list_[i] != nullptr && arr->list_[i] == nullptr) return false;
            if(list_[i] == nullptr && arr->list_[i] == nullptr) continue;
            if(!list_[i]->equals(arr->list_[i])) return false;
        }
        return true;
    }

    Object* clone() {
        return new Array(this);
    }

    // Returns number of elements in this
    size_t count() {
        return size_;
    }

    // Return the object at the index or null
    Object* get(size_t idx) {
        if(idx >= size_) return NULL;
        return list_[idx];
    }

    // Append the object to the end of this and return this
    Array* append(Object* obj) {
        if(size_ == capacity_) {
            capacity_ *= 2;
            Object** new_list = new Object*[capacity_];
            for (int i = 0; i < size_; ++i)
            {
                new_list[i] = list_[i];
            }
            delete[](list_);
            list_ = new_list;
        }
        list_[size_++] = obj;
        return this;
    }

    // Return index of object in this or -1 if the object is not contained
    int index(Object* obj)  {
        for (int i = 0; i < size_; ++i)
        {   
            if(list_[i] != nullptr) {
                if(list_[i]->equals(obj)) return i;
            } else if(obj == nullptr) return i;
        }
        return -1;
    }

    // Returns true if this contains object else false
    bool contains(Object* obj) {
        return index(obj) != -1;
    }

    // Appends a copy of all items of the other array to this and returns this
    Array* extend(const Array* arr) {
        for (int i = 0; i < arr->size_; ++i)
        {
            this->append(arr->list_[i]);
        }
        return this;
    }

    // Insert a copy of the object at the index if index from [0, this->count()] and return this
    Array* insert(size_t idx, Object* obj) {
        if(idx > size_) return this;
        append(obj);
        for (int i = size_ - 1; i > idx; --i)
        {
            Object* temp = list_[i - 1];
            list_[i - 1] = list_[i];
            list_[i] = temp;
        }
        return this;
    }

    Array* set(size_t idx, Object* obj) {
        if(idx >= size_) return this;
        list_[idx] = obj;
        return this;
    }

    // Return and remove object at the index or return null
    Object* pop(size_t idx) {
        if(idx >= size_) return NULL;
        Object* obj = list_[idx];
        for (int i = idx; i < size_ - 1; ++i)
        {
            Object* temp = list_[i];
            list_[i] = list_[i + 1];
            list_[i + 1] = temp;
        }
        size_--;
        list_[size_] = NULL;
        return obj;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    Array* remove(Object* obj) {
        int idx = index(obj);
        if(idx == -1) return this;
        delete(pop(idx));
        return this;
    }

    // Inplace reverse of this and return this
    Array* reverse() {
        Object** new_list = new Object*[capacity_];
        for (int i = 0; i < size_; ++i)
        {
            new_list[size_ - i - 1] = list_[i];
        }
        delete[](list_);
        list_ = new_list;
        return this;
    }

    // Delete contents of this
    void clear() {
        for (int i = 0; i < size_; ++i)
        {
            delete(list_[i]);
        }
        delete[](list_);
        size_ = 0;
        list_ = new Object*[capacity_];
    }
};

class StringArray : public SerializableObject {
public:
    size_t capacity_;
    size_t size_;
    String** vals_;

    StringArray(size_t capacity) {
        capacity_ = capacity;
        size_ = 0;
        vals_ = new String*[capacity_];
    }

    ~StringArray() {
        for (int i = 0; i < size_; ++i)
        {
            delete(vals_[i]);
        }
        delete[](vals_);
    }

    void append(String* str) {
        if(size_ == capacity_) assert(false);
        vals_[size_++] = new String(*str);
    }

    String* get(size_t idx) {
        if(idx >= size_) return nullptr;
        return vals_[idx];
    }

    char* serialize() {
        StrBuff buf;
        buf.c("SA|");
        buf.c(size_t_to_str(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(vals_[i]->c_str());
            buf.addc('|'); // delimiter for contained strings
        }

        String* str = buf.get();
        char* serialized = new char[str->size_];
        strncpy(serialized, str->c_str(), str->size_);
        delete(str);
        return serialized;
    }

    static StringArray* deserialize(char* serialized) {
        
        char* sizeBuf = new char[21];
        size_t idx = 3;
        for (; serialized[idx] != '|'; ++idx)
        {
            sizeBuf[idx - 3] = serialized[idx];
        }
        StringArray* sa = new StringArray(atoi(sizeBuf));
        delete[](sizeBuf);
        idx++;
        while(sa->size_ != sa->capacity_) {
            StrBuff sb;
            while(serialized[idx] != '|') {
                sb.addc(serialized[idx++]);
            }
            idx++;
            sa->append(sb.get());
        }

        return sa;
    }
};

class DoubleArray : public SerializableObject {
public:
    size_t capacity_;
    size_t size_;
    double* vals_;

    DoubleArray(size_t capacity) {
        capacity_ = capacity;
        size_ = 0;
        vals_ = new double[capacity_];
    }

    ~DoubleArray() {
        delete[](vals_);
    }

    void append(double d) {
        if(size_ == capacity_) assert(false);
        vals_[size_++] = d;
    }

    double get(size_t idx) {
        if(idx >= size_) return 0;
        return vals_[idx];
    }

    char* serialize() {
        StrBuff buf;
        buf.c("DA|");
        buf.c(size_t_to_str(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(double_to_str(vals_[i]));
            buf.addc('|'); // delimiter for contained doubles
        }

        String* str = buf.get();
        char* serialized = new char[str->size_];
        strncpy(serialized, str->c_str(), str->size_);
        delete(str);
        return serialized;
    }

    static DoubleArray* deserialize(char* serialized) {
        
        char* sizeBuf = new char[21];
        size_t idx = 3;
        for (; serialized[idx] != '|'; ++idx)
        {
            sizeBuf[idx - 3] = serialized[idx];
        }
        DoubleArray* sa = new DoubleArray(atoi(sizeBuf));
        delete[](sizeBuf);
        idx++;
        while(sa->size_ != sa->capacity_) {
            StrBuff sb;
            while(serialized[idx] != '|') {
                sb.addc(serialized[idx++]);
            }
            idx++;
            double d = atof(sb.get()->c_str());
            sa->append(d);
        }

        return sa;
    }
};