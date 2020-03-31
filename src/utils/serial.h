//lang:CwC

#pragma once

#include "object.h"

class SerialString {
public:
	char* data_;
	size_t size_;

	SerialString(const char* data, size_t size) {
		size_ = size;
		data_ = new char[size];
		memcpy(data_, data, size);
	}

	SerialString(char* data, size_t size) {
		size_ = size;
		data_ = new char[size];
		memcpy(data_, data, size);
	}

	~SerialString() {
		delete[](data_);
	}

	SerialString* clone() {
		return new SerialString(data_, size_);
	}
};

class Serializable {
public:
	virtual SerialString* serialize() { return nullptr; }
};

class SerializableObject : public Object, public Serializable {
public:
	static char* get_descriptor(char* serialized) {
		char* descriptor = new char[3];
		descriptor[0] = serialized[0];
		descriptor[1] = serialized[1];
		descriptor[2] = '\0';
		return descriptor;
	}
};