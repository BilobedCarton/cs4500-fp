//lang::CwC

#pragma once

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "utils/object.h"
#include "utils/string.h"

// Static values to specify the type of field this is.
static const size_t INT_TYPE = 0;
static const size_t FLOAT_TYPE = 1;
static const size_t STRING_TYPE = 2;
static const size_t BOOL_TYPE = 3;
static const size_t MISSING_TYPE = 4;
static const size_t INVALID = 5;

/**
 * @brief      This class describes a field.
 */
class Field : public Object {
public:
	size_t type_;
	char* value_; // owned.

	/**
	 * @brief      Constructs a new instance.
	 *
	 * @param[in]  type   The type of field
	 * @param      value  The value of the field represented as a char array
	 */
	Field(size_t type, char* value) : Object() {
		assert(type >= 0 && type < 5);
		if(type < 4) assert(value != NULL);
		if(type == BOOL_TYPE) assert(strcmp(value, "0") == 0 || strcmp(value, "1") == 0);
		if(type == STRING_TYPE) assert(strlen(value) < 255);
		// May want other checks for other data.

		type_ = type;
		if(value != NULL) {
			value_ = new char[strlen(value)];
			strcpy(value_, value);
		} else value_ = NULL;
	}

	/**
	 * @brief      Destroys the object.
	 */
	~Field() {
		delete[](value_);
	}

	/**
	 * @brief      Returns a character array representation of the object.
	 *
	 * @return     Character array representation of the object.
	 */
	char* to_char_arr() {
		if(value_ == NULL) return new char[3] {'<', '>', '\0'};
		size_t len = strlen(value_);
		char* arr = new char[len + 3];
		arr[0] = '<';
		strcpy(&arr[1], value_);
		arr[len + 1] = '>';
		arr[len + 2] = '\0';
		return arr;
	}

	/**
	 * @brief      Gets as integer.
	 *
	 * @return     value_ as integer.
	 */
	int get_as_int() {
		assert(type_ == INT_TYPE);
		return atoi(value_);
	}

	/**
	 * @brief      Gets as float.
	 *
	 * @return     value_ as float.
	 */
	float get_as_float() {
		assert(type_ == FLOAT_TYPE);
		return strtof(value_, NULL);
	}

	/**
	 * @brief      Gets as string.
	 *
	 * @return     value_ as string.
	 */
	String* get_as_string() {
		assert(type_ == STRING_TYPE);
		return new String(value_);
	}

	/**
	 * @brief      Gets as bool.
	 *
	 * @return     value_ as bool.
	 */
	bool get_as_bool() {
		assert(type_ == BOOL_TYPE);
		if(strcmp(value_, "0") == 0) return false;
		return true;
	}

	/**
	 * @brief      Determines if this is a missing.
	 *
	 * @return     True if missing, False otherwise.
	 */
	bool is_missing() {
		return type_ == MISSING_TYPE;
	}
};