// lang::CwC

#pragma once

#include <assert.h>
#include <string.h>
#include <stddef.h>

#include "utils/object.h"
#include "utils/string.h"


/**
 * @brief      List of generic Objects.
 */
class List : public Object {
public:
	Object** list_; // owned, since we're not readonly the items are unowned.
	size_t size_;
	size_t capacity_;


	/**
	 * @brief      Constructs a new List with base capacity 4.
	 */
	List() : Object() { 
		size_ = 0;
		capacity_ = 4;
		list_ = new Object*[capacity_];
	}

	/**
	 * @brief      Destroys the object.
	 */
	~List() {
		delete[] list_;
	}


	/**
	 * @brief      Pushes an object to the end of the list.
	 *
	 * @param      e     The Object to be appended
	 */
	void push_back(Object* e) {
		// Check if we have the space.
		if(this->size_ < this->capacity_) {
			this->list_[this->size_++] = e;
			return;
		}

		// We need to grow the internal list to fit the new Object.
		this->capacity_ *= 2;
		Object** new_list = new Object*[capacity_];
		for(int i = 0; i < this->size_; i++) {
			new_list[i] = this->list_[i];
		}
		delete[] this->list_;
		this->list_ = new_list;
		this->push_back(e);
	}

	/**
	 * @brief      Inserts the given Object e at position i in the list.
	 *
	 * @param[in]  i     The position where the object will be inserted.
	 * @param      e     The Object to be inserted.
	 */
	void add(size_t i, Object* e) {
		assert(i >= 0 && i <= size_);

		// append e.
		this->push_back(e);
		// swap adjacent elements starting at the end.
		for(int j = this->size_ - 1; j > i; j--) {
			Object* temp = this->list_[j];
			this->list_[j] = this->list_[j - 1];
			this->list_[j - 1] = temp;
		}
	}

	/**
	 * @brief      Adds all Objects in the given list at the given index.
	 *
	 * @param[in]  i     The index where the elements will be inserted.
	 * @param      c     The list containing the objects to be inserted.
	 */
	void add_all(size_t i, List* c) {
		for(int j = c->size_ - 1; j >= 0; j--) {
			this->add(i, c->get(j));
		}
	}

	/**
	 * @brief      Removes all of elements from this list
	 */
	void clear() {
		delete[] this->list_;
		this->size_ = 0;
		this->list_ = new Object*[capacity_];
	}

	/**
	 * @brief      Compares o with this list for equality.
	 *
	 * @param      o     the object to be compared with this one.
	 *
	 * @return     true if we're equal, false if not.
	 */
	bool equals(Object* o) {
		List* other = dynamic_cast<List *>(o);
		if (other == nullptr) return false;
		if(this->size_ != other->size_) return false;

		// iterate through the elements and check equality.
		for(int i = 0; i < this->size_; i++) {
			if(!this->list_[i]->equals(other->list_[i])) return false;
		}
		return true;
	}

	/**
	 * @brief      Gets the element at the specified index.
	 *
	 * @param[in]  index  The index
	 *
	 * @return     the Object at the given index.
	 */
	Object* get(size_t index) {
		assert(index < this->capacity_ && index >= 0);
		if(index >= this->size_) return nullptr; // return a null pointer for index beyond end of current list.
		return this->list_[index];
	}

	/**
	 * @brief      Returns the hash code value for this list.
	 *
	 * @return     an unsigned integer representing this data of this list.
	 */
	size_t hash() {
		// hash code formula based on answer to this stack overflow question:
		// https://stackoverflow.com/questions/2730865/how-do-i-calculate-a-good-hash-code-for-a-list-of-strings
		const int prime = 31;
		int result = 1;
		for(int i = 0; i < this->size_; i++) {
			result = result * prime + this->list_[i]->hash();
		}
		return result;
	}

	/**
	 * @brief      Returns a character array representation of the object.
	 *
	 * @return     Character array representation of the object.
	 */
	char* to_char_arr() {
		String* big_str = new String("");
		for (int i = 0; i < size_; ++i)
		{
			if(i != 0) big_str->str_[big_str->size_] = '\n';
			String* new_big = new String(big_str->str_);
			delete(big_str);

			String* temp = new String(this->get(i)->to_char_arr());

			big_str = new_big->concat(temp);

			delete(new_big);
			delete(temp);
		}
		char* arr = new char[big_str->size_+1];
		strcpy(arr, big_str->str_);
		delete(big_str);
		return arr;
	}

	/**
	 * @brief      Returns the index of the first occurrence of o, or >size() if not there
	 *
	 * @param      o     The object we're trying to find.
	 *
	 * @return     The index of the first occurence or size() if not there.
	 */
	size_t index_of(Object* o) {
		int i = 0;
		for(; i < this->size_; i++) {
			if(this->list_[i]->equals(o)) break;
		}
		return i;
	}


	/**
	 * @brief      Removes the specified element at index i.
	 *
	 * @param[in]  i     the index of the element to be removed.
	 *
	 * @return     The element previously located at the given index.
	 */
	Object* remove(size_t i) {
		Object* e = this->get(i);
		for(; i < this->size_ - 1; i++) {
			this->list_[i] = this->list_[i + 1];
		}
		this->list_[--(this->size_)] = nullptr;
		return e;
	}

	/**
	 * @brief      Replaces the element at i with e
	 *
	 * @param[in]  i     The index to be replaced
	 * @param      e     The new value
	 *
	 * @return     The old value stored at the index.
	 */
	Object* set(size_t i, String* e) {
		assert(i < this->size_);
		Object* old = this->get(i);
		this->list_[i] = e;
		return old;
	}

	/**
	 * @brief      Return the number of elements in the collection
	 *
	 * @return     the size of the list.
	 */
	size_t size() {
		return this->size_;
	}
};