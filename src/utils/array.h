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

class StringArray : public Array, public SerializableObject {
public:
    StringArray() : Array() {}

    StringArray(size_t capacity) : Array(capacity) { }

    StringArray(const StringArray* arr) : Array(arr) {  }

    ~StringArray() { }

    String* get(size_t idx) {
        return dynamic_cast<String *>(Array::get(idx));
    }

    char* serialize() {
        StrBuff buf;
        buf.c("SA|");
        buf.c(to_str<size_t>(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(list_[i]->c_str());
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
    double* list_;

    // Empty constructor
    DoubleArray() {
        size_ = 0;
        capacity_ = 4;
        list_ = new double[capacity_];
    }

    // Deep copy of other array
    DoubleArray(const DoubleArray* arr) {
        size_ = arr->size_;
        capacity_ = arr->capacity_;
        list_ = new double[capacity_];
        for (int i = 0; i < size_; ++i)
        {
            list_[i] = arr->list_[i];
        }
    }

    // capacity given constructor
    DoubleArray(size_t capacity) {
        size_ = 0;
        capacity_ = capacity;
        list_ = new double[capacity_];
    }

    // Clear contents of this before this is freed
    ~DoubleArray() {
        delete[](list_);
    }

    // Returns true if this equals the other object else false
    bool equals(Object* obj) {
        DoubleArray* arr = dynamic_cast<DoubleArray *>(obj);
        if(arr == NULL) return false;
        if(size_ != arr->size_) return false;
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] != (arr->list_[i])) return false;
        }
        return true;
    }

    Object* clone() {
        return new DoubleArray(this);
    }

    // Returns number of elements in this
    size_t count() {
        return size_;
    }

    // Return the object at the index or null
    double get(size_t idx) {
        if(idx >= size_) return 0.0;
        return list_[idx];
    }

    // Append a copy of the object to the end of this and return this
    DoubleArray* append(double obj) {
        if(size_ == capacity_) {
            capacity_ *= 2;
            double* new_list = new double[capacity_];
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
    int index(const double obj) {
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] == obj) return i;
        }
        return -1;
    }

    // Returns true if this contains object else false
    bool contains(const double obj) {
        return index(obj) != -1;
    }

    // Appends a copy of all items of the other array to this and returns this
    DoubleArray* extend(const DoubleArray* arr) {
        for (int i = 0; i < arr->size_; ++i)
        {
            this->append(arr->list_[i]);
        }
        return this;
    }
    // Insert a copy of the object at the index if index from [0, this->count()]
    // and return this
    DoubleArray* insert(size_t idx, double obj) {
        if(idx > size_) return this;
        append(obj);
        for (int i = size_ - 1; i > idx; --i)
        {
            double temp = list_[i - 1];
            list_[i - 1] = list_[i];
            list_[i] = temp;
        }
        return this;
    }

    DoubleArray* set(size_t idx, double obj) {
        if(idx >= size_) return this;
        list_[idx] = obj;
        return this;
    }

    // Return and remove object at the index or return null
    double pop(size_t idx) {
        assert(idx < size_);
        double obj = list_[idx];
        for (int i = idx; i < size_ - 1; ++i)
        {
            double temp = list_[i];
            list_[i] = list_[i + 1];
            list_[i + 1] = temp;
        }
        size_--;
        list_[size_] = 0;
        return obj;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    DoubleArray* remove(const double obj) {
        int idx = index(obj);
        if(idx == -1) return this;
        pop(idx);
        return this;
    }
    // Inplace reverse of this and return this
    DoubleArray* reverse() {
        double* new_list = new double[capacity_];
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
        delete[](list_);
        size_ = 0;
        list_ = new double[capacity_];
    }

    char* serialize() {
        StrBuff buf;
        buf.c("DA|");
        buf.c(to_str<size_t>(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(to_str<double>(list_[i]));
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

// Resizeable array with amortized appending to end of list and constant
// time retrieval
class IntArray : public Object {
   public:
        size_t size_;
        size_t capacity_;
        int* list_;

    // Empty constructor
    IntArray() {
        size_ = 0;
        capacity_ = 4;
        list_ = new int[capacity_];
    }

    // Deep copy of other array
    IntArray(const IntArray* arr) {
        size_ = arr->size_;
        capacity_ = arr->capacity_;
        list_ = new int[capacity_];
        for (int i = 0; i < size_; ++i)
        {
            list_[i] = arr->list_[i];
        }
    }

    // capacity given constructor
    IntArray(size_t capacity) {
        size_ = 0;
        capacity_ = capacity;
        list_ = new int[capacity_];
    }

    // Clear contents of this before this is freed
    ~IntArray() {
        delete[](list_);
    }

    // Returns hash code for this
    size_t hash() {
        if(size_ == 0) { return 0; }
        size_t h = size_;
        for (int i = 0; i < size_; ++i)
        {
            list_[i] >= 0 ? h += list_[i] : h-= list_[i];
        }
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(Object* obj) {
        IntArray* arr = dynamic_cast<IntArray *>(obj);
        if(arr == NULL) return false;
        if(size_ != arr->size_) return false;
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] != (arr->list_[i])) return false;
        }
        return true;
    }

    Object* clone() {
        return new IntArray(this);
    }

    // Returns number of elements in this
    size_t count() {
        return size_;
    }

    // Return the object at the index or null
    int get(size_t idx) {
        if(idx >= size_) return 0;
        return list_[idx];
    }

    // Append a copy of the object to the end of this and return this
    IntArray* append(int obj) {
        if(size_ == capacity_) {
            capacity_ *= 2;
            int* new_list = new int[capacity_];
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
    int index(const int obj) {
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] == obj) return i;
        }
        return -1;
    }

    // Returns true if this contains object else false
    bool contains(const int obj) {
        return index(obj) != -1;
    }

    // Appends a copy of all items of the other array to this and returns this
    IntArray* extend(const IntArray* arr) {
        for (int i = 0; i < arr->size_; ++i)
        {
            this->append(arr->list_[i]);
        }
        return this;
    }
    // Insert a copy of the object at the index if index from [0, this->count()]
    // and return this
    IntArray* insert(size_t idx, int obj) {
        if(idx > size_) return this;
        append(obj);
        for (int i = size_ - 1; i > idx; --i)
        {
            int temp = list_[i - 1];
            list_[i - 1] = list_[i];
            list_[i] = temp;
        }
        return this;
    }

    IntArray* set(size_t idx, int obj) {
        if(idx >= size_) return this;
        list_[idx] = obj;
        return this;
    }

    // Return and remove object at the index or return null
    int pop(size_t idx) {
        assert(idx < size_);
        int obj = list_[idx];
        for (int i = idx; i < size_ - 1; ++i)
        {
            int temp = list_[i];
            list_[i] = list_[i + 1];
            list_[i + 1] = temp;
        }
        size_--;
        list_[size_] = 0;
        return obj;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    IntArray* remove(const int obj) {
        int idx = index(obj);
        if(idx == -1) return this;
        pop(idx);
        return this;
    }
    // Inplace reverse of this and return this
    IntArray* reverse() {
        int* new_list = new int[capacity_];
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
        delete[](list_);
        size_ = 0;
        list_ = new int[capacity_];
    }

    char* serialize() {
        StrBuff buf;
        buf.c("IA|");
        buf.c(to_str<size_t>(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(to_str<int>(list_[i]));
            buf.addc('|'); // delimiter for contained doubles
        }

        String* str = buf.get();
        char* serialized = new char[str->size_];
        strncpy(serialized, str->c_str(), str->size_);
        delete(str);
        return serialized;
    }

    static IntArray* deserialize(char* serialized) {
        char* sizeBuf = new char[21];
        size_t idx = 3;
        for (; serialized[idx] != '|'; ++idx)
        {
            sizeBuf[idx - 3] = serialized[idx];
        }
        IntArray* sa = new IntArray(atoi(sizeBuf));
        delete[](sizeBuf);
        idx++;
        while(sa->size_ != sa->capacity_) {
            StrBuff sb;
            while(serialized[idx] != '|') {
                sb.addc(serialized[idx++]);
            }
            idx++;
            int d = atoi(sb.get()->c_str());
            sa->append(d);
        }

        return sa;
    }
};

// Resizeable array with amortized appending to end of list and constant
// time retrieval
class FloatArray : public Object {
   public:
        size_t size_;
        size_t capacity_;
        float* list_;

    // Empty constructor
    FloatArray() {
        size_ = 0;
        capacity_ = 4;
        list_ = new float[capacity_];
    }

    // Deep copy of other array
    FloatArray(const FloatArray* arr) {
        size_ = arr->size_;
        capacity_ = arr->capacity_;
        list_ = new float[capacity_];
        for (int i = 0; i < size_; ++i)
        {
            list_[i] = arr->list_[i];
        }
    }

    // capacity given constructor
    FloatArray(size_t capacity) {
        size_ = 0;
        capacity_ = capacity;
        list_ = new float[capacity_];
    }

    // Clear contents of this before this is freed
    ~FloatArray() {
        delete[](list_);
    }

    // Returns hash code for this
    size_t hash() {
        if(size_ == 0) { return 0; }
        float h = size_;
        for (int i = 0; i < size_; ++i)
        {
            h = list_[i] >= 0 ? h + list_[i] : h - list_[i];
        }
        size_t hv;
        memcpy( &hv, &h, sizeof( float ) );
        return hv & 0xfffff000;
    }

    // Returns true if this equals the other object else false
    bool equals(Object* obj) {
        FloatArray* arr = dynamic_cast<FloatArray *>(obj);
        if(arr == NULL) return false;
        if(size_ != arr->size_) return false;
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] != (arr->list_[i])) return false;
        }
        return true;
    }

    Object* clone() {
        return new FloatArray(this);
    }

    // Returns number of elements in this
    size_t count() {
        return size_;
    }

    // Return the object at the index or null
    float get(size_t idx) {
        if(idx >= size_) return 0;
        return list_[idx];
    }

    // Append a copy of the object to the end of this and return this
    FloatArray* append(float obj) {
        if(size_ == capacity_) {
            capacity_ *= 2;
            float* new_list = new float[capacity_];
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
    int index(const float obj) {
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] == obj) return i;
        }
        return -1;
    }

    // Returns true if this contains object else false
    bool contains(const float obj) {
        return index(obj) != -1;
    }

    // Appends a copy of all items of the other array to this and returns this
    FloatArray* extend(const FloatArray* arr) {
        for (int i = 0; i < arr->size_; ++i)
        {
            this->append(arr->list_[i]);
        }
        return this;
    }

    // Insert a copy of the object at the index if index from [0, this->count()]
    // and return this
    FloatArray* insert(size_t idx, float obj) {
        if(idx > size_) return this;
        append(obj);
        for (int i = size_ - 1; i > idx; --i)
        {
            float temp = list_[i - 1];
            list_[i - 1] = list_[i];
            list_[i] = temp;
        }
        return this;
    }

    FloatArray* set(size_t idx, float obj) {
        if(idx >= size_) return this;
        list_[idx] = obj;
        return this;
    }

    // Return and remove object at the index or return null
    float pop(size_t idx) {
        assert(idx < size_);
        float obj = list_[idx];
        for (int i = idx; i < size_ - 1; ++i)
        {
            float temp = list_[i];
            list_[i] = list_[i + 1];
            list_[i + 1] = temp;
        }
        size_--;
        list_[size_] = 0;
        return obj;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    FloatArray* remove(const float obj) {
        int idx = index(obj);
        if(idx == -1) return this;
        pop(idx);
        return this;
    }

    // Inplace reverse of this and return this
    FloatArray* reverse() {
        float* new_list = new float[capacity_];
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
        delete[](list_);
        size_ = 0;
        list_ = new float[capacity_];
    }

    char* serialize() {
        StrBuff buf;
        buf.c("IA|");
        buf.c(to_str<size_t>(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(to_str<float>(list_[i]));
            buf.addc('|'); // delimiter for contained doubles
        }

        String* str = buf.get();
        char* serialized = new char[str->size_];
        strncpy(serialized, str->c_str(), str->size_);
        delete(str);
        return serialized;
    }

    static FloatArray* deserialize(char* serialized) {
        char* sizeBuf = new char[21];
        size_t idx = 3;
        for (; serialized[idx] != '|'; ++idx)
        {
            sizeBuf[idx - 3] = serialized[idx];
        }
        FloatArray* sa = new FloatArray(atoi(sizeBuf));
        delete[](sizeBuf);
        idx++;
        while(sa->size_ != sa->capacity_) {
            StrBuff sb;
            while(serialized[idx] != '|') {
                sb.addc(serialized[idx++]);
            }
            idx++;
            double d = atof(sb.get()->c_str());
            sa->append((float)d);
        }

        return sa;
    }
};

// Resizeable array with amortized appending to end of list and constant
// time retrieval
class BoolArray : public Object {
   public:
        size_t size_;
        size_t capacity_;
        bool* list_;

    // Empty constructor
    BoolArray() {
        size_ = 0;
        capacity_ = 4;
        list_ = new bool[capacity_];
    }

    // Deep copy of other array
    BoolArray(const BoolArray* arr) {
        size_ = arr->size_;
        capacity_ = arr->capacity_;
        list_ = new bool[capacity_];
        for (int i = 0; i < size_; ++i)
        {
            list_[i] = arr->list_[i];
        }
    }

    // capacity given constructor
    BoolArray(size_t capacity) {
        size_ = 0;
        capacity_ = capacity;
        list_ = new bool[capacity_];
    }

    // Clear contents of this before this is freed
    ~BoolArray() {
        delete[](list_);
    }

    // Returns hash code for this
    size_t hash() {
        if(size_ == 0) { return 0; }
        size_t h = size_;
        for (int i = 0; i < size_; ++i)
        {
            h *= 10;
            h += list_[i] ? 1 : 0;
        }
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(Object* obj) {
        BoolArray* arr = dynamic_cast<BoolArray *>(obj);
        if(arr == NULL) return false;
        if(size_ != arr->size_) return false;
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] != (arr->list_[i])) return false;
        }
        return true;
    }

    Object* clone() {
        return new BoolArray(this);
    }

    // Returns number of elements in this
    size_t count() {
        return size_;
    }

    // Return the object at the index or null
    bool get(size_t idx) {
        if(idx >= size_) return false;
        return list_[idx];
    }

    // Append a copy of the object to the end of this and return this
    BoolArray* append(bool obj) {
        if(size_ == capacity_) {
            capacity_ *= 2;
            bool* new_list = new bool[capacity_];
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
    int index(const bool obj) {
        for (int i = 0; i < size_; ++i)
        {
            if(list_[i] == obj) return i;
        }
        return -1;
    }

    // Returns true if this contains object else false
    bool contains(const bool obj) {
        return index(obj) != -1;
    }

    // Appends a copy of all items of the other array to this and returns this
    BoolArray* extend(const BoolArray* arr) {
        for (int i = 0; i < arr->size_; ++i)
        {
            this->append(arr->list_[i]);
        }
        return this;
    }

    // Insert a copy of the object at the index if index from [0, this->count()]
    // and return this
    BoolArray* insert(size_t idx, bool obj) {
        if(idx > size_) return this;
        append(obj);
        for (int i = size_ - 1; i > idx; --i)
        {
            bool temp = list_[i - 1];
            list_[i - 1] = list_[i];
            list_[i] = temp;
        }
        return this;
    }

    BoolArray* set(size_t idx, bool obj) {
        if(idx >= size_) return this;
        list_[idx] = obj;
        return this;
    }

    // Return and remove object at the index or return null
    bool pop(size_t idx) {
        assert(idx < size_);
        bool obj = list_[idx];
        for (int i = idx; i < size_ - 1; ++i)
        {
            bool temp = list_[i];
            list_[i] = list_[i + 1];
            list_[i + 1] = temp;
        }
        size_--;
        list_[size_] = false;
        return obj;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    BoolArray* remove(const bool obj) {
        int idx = index(obj);
        if(idx == -1) return this;
        pop(idx);
        return this;
    }

    // Inplace reverse of this and return this
    BoolArray* reverse() {
        bool* new_list = new bool[capacity_];
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
        delete[](list_);
        size_ = 0;
        list_ = new bool[capacity_];
    }

    char* serialize() {
        StrBuff buf;
        buf.c("BA|");
        buf.c(to_str<size_t>(size_));
        buf.addc('|');

        for (int i = 0; i < size_; ++i)
        {
            buf.c(to_str<size_t>(list_[i]));
            buf.addc('|'); // delimiter for contained doubles
        }

        String* str = buf.get();
        char* serialized = new char[str->size_];
        strncpy(serialized, str->c_str(), str->size_);
        delete(str);
        return serialized;
    }

    static BoolArray* deserialize(char* serialized) {
        char* sizeBuf = new char[21];
        size_t idx = 3;
        for (; serialized[idx] != '|'; ++idx)
        {
            sizeBuf[idx - 3] = serialized[idx];
        }
        BoolArray* sa = new BoolArray(atoi(sizeBuf));
        delete[](sizeBuf);
        idx++;
        while(sa->size_ != sa->capacity_) {
            StrBuff sb;
            while(serialized[idx] != '|') {
                sb.addc(serialized[idx++]);
            }
            idx++;
            bool d = atoi(sb.get()->c_str()) == 1;
            sa->append(d);
        }

        return sa;
    }
};