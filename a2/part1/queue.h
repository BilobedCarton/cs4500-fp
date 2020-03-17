// Andrew Stam, Hak Joon Lee - CS 4500 Assignment 1
#pragma once
#include <stdio.h>
#include "object.h"
#include "string.h"

// Represents a queue of Objects
class Queue {
	public:
		size_t size_;
		size_t capacity_;
		Object** list_; // elements are not owned.

		// Create a new Queue of Objects
		Queue() {
			size_ = 0;
			capacity_ = 4;
			list_ = new Object*[capacity_];
		}

		// Destruct the Queue
		virtual ~Queue() {
			delete[](list_);
		}

		// Insert the given Object at the end of the Queue
		void enqueue(Object* o)  {
			assert(o != nullptr);
			if(size_ == capacity_) {
				capacity_ *= 2;
				Object** new_list = new Object*[capacity_];
				for (size_t i = 0; i < size_; ++i)
				{
					new_list[i] = list_[i];
				}
				delete[](list_);
				list_ = new_list;
			}
			list_[size_++] = o;
		}	
	
		// Remove and return the Object at the front of the Queue
		// If the queue is empty, return NULL
		Object* dequeue() {
			if(size_ == 0) return NULL;
			Object* front = list_[0];
			for (size_t i = 1; i < size_; ++i)
			{
				list_[i - 1] = list_[i];
			}
			list_[size_ - 1] = nullptr;
			size_--;
			return front;
		}

		// Clear the elements of the Queue
		virtual void clear() {
			for (size_t i = 0; i < size_; ++i)
			{
				list_[i] = nullptr;
			}
			size_ = 0;
		}

		// Check if the Queue is empty
		virtual bool isEmpty() {
			return size_ == 0;
		}

		// Return the Object at the front of the Queue without removing the element
		// If the queue is empty, return NULL
		Object* peek() {
			if(size_ == 0) return NULL;
			return list_[0];
		}

		// Return the size of the Queue
		virtual size_t size() {
			return size_;
		}

		// Return the Object at the given index without removing the element
		// If out of bounds, return NULL
		Object* get(size_t index) {
			if(index >= size_) return NULL;
			return list_[index];
		}
};

// Represents a queue of Strings
class StrQueue : public Queue {
	public:
		// Create a new Queue of Strings
		StrQueue() : Queue() { }

		// Destruct the StrQueue
		~StrQueue() { }
		
		// Insert the given String at the end of the Queue
		void enqueue(String* o) {
			Queue::enqueue(o);
		}	
	
		// Remove and return the String at the front of the Queue
		// If the queue is empty, return NULL
		String* dequeue() {
			return dynamic_cast<String *>(Queue::dequeue());
		}

		// Return the String at the front of the Queue without removing the element
		// If the queue is empty, return NULL
		String* peek() {
			return dynamic_cast<String *>(Queue::peek());
		}

		// Return the String at the given index without removing the element
		// If out of bounds, return NULL
		String* get(size_t index) {
			return dynamic_cast<String *>(Queue::get(index));
		}
};
