//lang:CwC

#pragma once

#include "object.h"

class SerializableObject : public Object {
public:
	virtual char* serialize() { return nullptr; }

	static char* get_descriptor(char* serialized) {
		char* descriptor = new char[3];
		descriptor[0] = serialized[0];
		descriptor[1] = serialized[1];
		descriptor[2] = '\0';
		return descriptor;
	}
};