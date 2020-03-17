//lang::CwC

#pragma once

#include "object.h"

#include <stdio.h>
#include <string.h>

class String: public Object
{
public :
    int length;
    char* data;
public:
    String() { 
        length = 0;
        data = new char[1];
        data[0] = '\0';
    }

    String(int capacity) {
        length = capacity;
        data = new char[length + 1];
        data[length] = '\0';
    }

    String(const char* s) {
        length = strlen(s);
        data = new char[length + 1];
        strcpy(data, s);
        data[length] = '\0';
    }

    ~String() {
        delete[](data);
    }

public:
    size_t hash() {
        size_t h = 0;
        for (size_t i = 0; i < length; ++i)
        {
            h += 31 * data[i];
        }
        return h;
    }

    bool equals(Object* object) {
        String* cast = dynamic_cast<String *>(object);
        if(cast == nullptr) return false;
        return strcmp(data, cast->data) == 0;
    }

    String* concat(String* s) {
        size_t big_size = length + s->length;
        char* big_str = new char[big_size + 1];
        for (size_t i = 0; i < big_size; ++i)
        {
            if(i < length) big_str[i] = data[i];
            else big_str[i] = s->data[i - length];
        }
        big_str[big_size] = '\0';
        String* ret = new String(big_str);
        delete[](big_str);
        return ret;
    }

    int compareTo(String* s) {
        return strcmp(data, s->data);
    }

    Object* clone() {
        return new String(data);
    }
};
