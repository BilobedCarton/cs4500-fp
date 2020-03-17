// lang::Cpp

#pragma once

#include <iostream>
#include <string>

#include "object.h"
#include "list.h"

void print(char c) {
	std::cout << c;
}

void print(size_t c) {
	std::cout << c;
}

void print(int c) {
	std::cout << c;
}

void print(float c) {
	std::cout << c;
}

void print(const char* str) {
	std::cout << str;
}

void print(Object* o) {
	std::cout << o->to_char_arr();
}

void println(Object* o) {
	print(o);
	std::cout << "\n";
}

void println(char c) {
	print(c);
	std::cout << "\n";
}

void println(size_t c) {
	print(c);
	std::cout << "\n";
}

void println(int c) {
	print(c);
	std::cout << "\n";
}

void println(float c) {
	print(c);
	std::cout << "\n";
}

void println(const char* str) {
	print(str);
	std::cout << "\n";
}

void print(List* l) {
	for (int i = 0; i < l->size_; ++i) {
		println(l->get(i));
	}
}

/**
 * @brief      Returns a char array representation of a float.
 *
 * @param[in]  f     a float
 *
 * @return     char array representation of the float.
 */
const char* float_to_string(float f) {
	return std::to_string(f).c_str();
}

const char* int_to_string(int i) {
	return std::to_string(i).c_str();
}