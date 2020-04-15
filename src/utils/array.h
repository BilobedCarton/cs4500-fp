// lang::CwC
#pragma once

#include <cstddef>
#include <string.h>
#include <assert.h>

#include "object.h"
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
        list_[size_++] = obj->clone();
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
};