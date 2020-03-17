#pragma once

#include <assert.h>
#include <stdlib.h>

#include "object.h"
#include "array.h"
#include "string.h"

const size_t CHILD_SIZE = 128;

/**
 * @brief      This class describes a string array wrapper to allow for O(1) access without resizing and copying payload.
 */
class StringArrayWrapper : public Object {
public:
	Array** _data;
	size_t _capacity;
	size_t _size;

	/**
	 * @brief      Constructs a new instance.
	 */
	StringArrayWrapper() {
		_capacity = 4;
		_size = 0;
		_data = new Array*[_capacity];
		_data[_size++] = new Array(CHILD_SIZE);
	}

	/**
	 * @brief      Destroys the object.
	 */
	~StringArrayWrapper() {
		for (int i = 0; i < _size; ++i)
		{
			delete(_data[i]);
		}
		delete[](_data);
	}

	/**
	 * @brief      grows the wrapper to indirectly store more data
	 * 			   copies the pointers to the arrays storing the payload, but not the payload itself
	 */
	void grow() {
		// do we need to grow?
		if(_data[_size - 1]->size_ == CHILD_SIZE) {
			// we need another array.
			if(_size == _capacity) {
				// we need to resize our wrapping array
				_capacity *= 2;
				Array** new_data = new Array*[_capacity];
				memcpy(new_data, _data, _size * sizeof(Array*));
				delete[](_data);
				_data = new_data;
			}
			_data[_size++] = new Array(CHILD_SIZE);
		}
	}

	/**
	 * @brief      Gets the string at specified index.
	 *
	 * @param[in]  idx   The index
	 *
	 * @return     the corresponding string
	 */
	String* get(size_t idx) {
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);
		assert(child_idx < _data[data_idx]->size_);

		return dynamic_cast<String*>(_data[data_idx]->get(child_idx));
	}

	/**
	 * @brief      adds the given string to the end of the stored data
	 *
	 * @param      obj   The string to be added
	 */
	void append(String* obj) {
		grow();
		_data[_size - 1]->append(obj);
	}

	/**
	 * @brief      sets the value at the given index to the given value
	 *
	 * @param[in]  idx   The index
	 * @param      obj   The new value
	 */
	void set(size_t idx, String* obj) {
		grow();
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);

		if(child_idx == 0 && _data[data_idx]->size_ == 0) _data[data_idx]->append(obj); // handle set w/ index input 0
		assert(child_idx < _data[data_idx]->size_);

		_data[data_idx]->set(child_idx, obj);
	}

	/**
	 * @brief      returns the number of data elements stored
	 *
	 * @return     the number of elements
	 */
	size_t size() {
		size_t size = 0;
		for (int i = 0; i < _size; ++i)
		{
			size += _data[i]->size_;
		}
		return size;
	}

    bool equals(Object  * other) { 
    	StringArrayWrapper* cast = dynamic_cast<StringArrayWrapper *>(other);
    	if(cast == nullptr) return false;
    	if(_size != cast->_size) return false;
    	if(size() != cast->size()) return false;
    	for (int i = 0; i < _size; ++i)
    	{
    		if(!_data[i]->equals(cast->_data[i])) return false;
    	}
    	return true;
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() { 
    	StringArrayWrapper* clone = new StringArrayWrapper();
    	for (int i = 0; i < _size; ++i)
    	{
    		for (int j = 0; j < _data[i]->size_; ++j)
    		{
    			clone->append(get(j));
    		}
    	}
    	return clone;
    }
};

/* Primitive data type implementations below function almost identically to the string based definition. */

class IntArrayWrapper : public Object {
	public:
	IntArray** _data;
	size_t _capacity;
	size_t _size;

	IntArrayWrapper() {
		_capacity = 4;
		_size = 0;
		_data = new IntArray*[_capacity];
		_data[_size++] = new IntArray(CHILD_SIZE);
	}

	~IntArrayWrapper() {
		for (int i = 0; i < _size; ++i)
		{
			delete(_data[i]);
		}
		delete[](_data);
	}

	void grow() {
		// do we need to grow?
		if(_data[_size - 1]->size_ == CHILD_SIZE) {
			// we need another array.
			if(_size == _capacity) {
				// we need to resize our wrapping array
				_capacity *= 2;
				IntArray** new_data = new IntArray*[_capacity];
				memcpy(new_data, _data, _size * sizeof(IntArray*));
				delete[](_data);
				_data = new_data;
			}
			_data[_size++] = new IntArray(CHILD_SIZE);
		}
	}

	int get(size_t idx) {
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);
		assert(child_idx < _data[data_idx]->size_);

		return _data[data_idx]->get(child_idx);
	}

	void append(int obj) {
		grow();
		_data[_size - 1]->append(obj);
	}

	void set(size_t idx, int obj) {
		grow();
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);

		if(child_idx == 0 && _data[data_idx]->size_ == 0) _data[data_idx]->append(obj); // handle set w/ index input 0
		assert(child_idx < _data[data_idx]->size_);

		_data[data_idx]->set(child_idx, obj);
	}

	size_t size() {
		size_t size = 0;
		for (int i = 0; i < _size; ++i)
		{
			size += _data[i]->size_;
		}
		return size;
	}

    bool equals(Object  * other) { 
    	IntArrayWrapper* cast = dynamic_cast<IntArrayWrapper *>(other);
    	if(cast == nullptr) return false;
    	if(_size != cast->_size) return false;
    	if(size() != cast->size()) return false;
    	for (int i = 0; i < _size; ++i)
    	{
    		if(!_data[i]->equals(cast->_data[i])) return false;
    	}
    	return true;
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() { 
    	IntArrayWrapper* clone = new IntArrayWrapper();
    	for (int i = 0; i < _size; ++i)
    	{
    		for (int j = 0; j < _data[i]->size_; ++j)
    		{
    			clone->append(_data[i]->get(j));
    		}
    	}
    	return clone;
    }
};

class FloatArrayWrapper : public Object {
	public:
	FloatArray** _data;
	size_t _capacity;
	size_t _size;

	FloatArrayWrapper() {
		_capacity = 4;
		_size = 0;
		_data = new FloatArray*[_capacity];
		_data[_size++] = new FloatArray(CHILD_SIZE);
	}

	~FloatArrayWrapper() {
		for (int i = 0; i < _size; ++i)
		{
			delete(_data[i]);
		}
		delete[](_data);
	}

	void grow() {
		// do we need to grow?
		if(_data[_size - 1]->size_ == CHILD_SIZE) {
			// we need another array.
			if(_size == _capacity) {
				// we need to resize our wrapping array
				_capacity *= 2;
				FloatArray** new_data = new FloatArray*[_capacity];
				memcpy(new_data, _data, _size * sizeof(FloatArray*));
				delete[](_data);
				_data = new_data;
			}
			_data[_size++] = new FloatArray(CHILD_SIZE);
		}
	}

	float get(size_t idx) {
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);
		assert(child_idx < _data[data_idx]->size_);

		return _data[data_idx]->get(child_idx);
	}

	void append(float obj) {
		grow();
		_data[_size - 1]->append(obj);
	}

	void set(size_t idx, float obj) {
		grow();
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);

		if(child_idx == 0 && _data[data_idx]->size_ == 0) _data[data_idx]->append(obj); // handle set w/ index input 0
		assert(child_idx < _data[data_idx]->size_);

		_data[data_idx]->set(child_idx, obj);
	}

	size_t size() {
		size_t size = 0;
		for (int i = 0; i < _size; ++i)
		{
			size += _data[i]->size_;
		}
		return size;
	}

    bool equals(Object  * other) { 
    	FloatArrayWrapper* cast = dynamic_cast<FloatArrayWrapper *>(other);
    	if(cast == nullptr) return false;
    	if(_size != cast->_size) return false;
    	if(size() != cast->size()) return false;
    	for (int i = 0; i < _size; ++i)
    	{
    		if(!_data[i]->equals(cast->_data[i])) return false;
    	}
    	return true;
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() { 
    	FloatArrayWrapper* clone = new FloatArrayWrapper();
    	for (int i = 0; i < _size; ++i)
    	{
    		for (int j = 0; j < _data[i]->size_; ++j)
    		{
    			clone->append(_data[i]->get(j));
    		}
    	}
    	return clone;
    }
};

class BoolArrayWrapper : public Object {
	public:
	BoolArray** _data;
	size_t _capacity;
	size_t _size;

	BoolArrayWrapper() {
		_capacity = 4;
		_size = 0;
		_data = new BoolArray*[_capacity];
		_data[_size++] = new BoolArray(CHILD_SIZE);
	}

	~BoolArrayWrapper() {
		for (int i = 0; i < _size; ++i)
		{
			delete(_data[i]);
		}
		delete[](_data);
	}

	void grow() {
		// do we need to grow?
		if(_data[_size - 1]->size_ == CHILD_SIZE) {
			// we need another array.
			if(_size == _capacity) {
				// we need to resize our wrapping array
				_capacity *= 2;
				BoolArray** new_data = new BoolArray*[_capacity];
				memcpy(new_data, _data, _size * sizeof(BoolArray*));
				delete[](_data);
				_data = new_data;
			}
			_data[_size++] = new BoolArray(CHILD_SIZE);
		}
	}

	bool get(size_t idx) {
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);
		assert(child_idx < _data[data_idx]->size_);

		return _data[data_idx]->get(child_idx);
	}

	void append(bool obj) {
		grow();
		_data[_size - 1]->append(obj);
	}

	void set(size_t idx, bool obj) {
		grow();
		size_t child_idx = idx % CHILD_SIZE;
		size_t data_idx = (idx - child_idx) / CHILD_SIZE;
		assert(data_idx < _size);

		if(child_idx == 0 && _data[data_idx]->size_ == 0) _data[data_idx]->append(obj); // handle set w/ index input 0
		assert(child_idx < _data[data_idx]->size_);

		_data[data_idx]->set(child_idx, obj);
	}

	size_t size() {
		size_t size = 0;
		for (int i = 0; i < _size; ++i)
		{
			size += _data[i]->size_;
		}
		return size;
	}

    bool equals(Object  * other) { 
    	BoolArrayWrapper* cast = dynamic_cast<BoolArrayWrapper *>(other);
    	if(cast == nullptr) return false;
    	if(_size != cast->_size) return false;
    	if(size() != cast->size()) return false;
    	for (int i = 0; i < _size; ++i)
    	{
    		if(!_data[i]->equals(cast->_data[i])) return false;
    	}
    	return true;
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() { 
    	BoolArrayWrapper* clone = new BoolArrayWrapper();
    	for (int i = 0; i < _size; ++i)
    	{
    		for (int j = 0; j < _data[i]->size_; ++j)
    		{
    			clone->append(_data[i]->get(j));
    		}
    	}
    	return clone;
    }
};