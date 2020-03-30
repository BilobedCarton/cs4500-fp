#pragma once

#include "../utils/object.h"
#include "../utils/string.h"
#include "../utils/primitivearray.h"

// page size divided by type size
#define INT_CHUNK_SIZE 4096 / sizeof(int) 
#define BOOL_CHUNK_SIZE 4096 / sizeof(bool) 
#define DOUBLE_CHUNK_SIZE 4096 / sizeof(double) 
#define STRING_CHUNK_SIZE 4096 / sizeof(String *) 

class IntColumn;
class StringColumn;
class BoolColumn;
class DoubleColumn;
/**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class Column : public Object {
 public:
 
  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn* as_int() { return nullptr; }
  virtual BoolColumn*  as_bool() { return nullptr; }
  virtual DoubleColumn* as_double() { return nullptr; }
  virtual StringColumn* as_string() { return nullptr; }
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  virtual void push_back(int val) { return; }
  virtual void push_back(bool val) { return; }
  virtual void push_back(double val) { return; }
  virtual void push_back(String* val) { return; }
 
 /** Returns the number of elements in the column. */
  virtual size_t size() { return 0; }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
  char get_type() { 
    if(as_int() != nullptr) return 'I';
    if(as_bool() != nullptr) return 'B';
    if(as_double() != nullptr) return 'F';
    if(as_string() != nullptr) return 'S';
    return '\0';
  }

  // Checks if the stored data is the same as the given object's stored data
  virtual bool data_equals(Object * other) { return false; }
};
 
/*************************************************************************
 * IntColumn::
 * Holds primitive int values, unwrapped.
 */
class IntColumn : public Column {
 public:
  PrimitiveArray<int>* _data;

  IntColumn() { _data = new PrimitiveArray<int>(INT_CHUNK_SIZE); }

  IntColumn(int n, ...) : IntColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i)
    {
      _data->push_back(va_arg(args, int));
    }
    va_end(args);
  }

  ~IntColumn() {
    delete(_data);
  }

  void push_back(int val) { this->as_int()->_data->push_back(val); }

  int get(size_t idx) { return _data->get(idx); }
  IntColumn* as_int() { return dynamic_cast<IntColumn *>(this); }
  void set(size_t idx, int val) { _data->set(idx, val); }
  size_t size() { return _data->count(); }

  bool data_equals(Object  * other) {
    IntColumn* cast = dynamic_cast<IntColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    IntColumn* clone = new IntColumn();
    clone->_data = _data->clone();
    return clone;
  }
};
 
// Other primitive column classes similar...

/*************************************************************************
 * FloatColumn::
 * Holds primitive double values, unwrapped.
 */
class DoubleColumn : public Column {
 public:
  PrimitiveArray<double>* _data;

  DoubleColumn() { _data = new PrimitiveArray<double>(DOUBLE_CHUNK_SIZE); }

  DoubleColumn(int n, ...) : DoubleColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i)
    {
      _data->push_back(va_arg(args, double));
    }
    va_end(args);
  }

  ~DoubleColumn() {
    delete(_data);
  }

  void push_back(double val) { this->as_double()->_data->push_back(val); }

  double get(size_t idx) { return _data->get(idx); }
  DoubleColumn* as_double() { return dynamic_cast<DoubleColumn *>(this); }
  void set(size_t idx, float val) { _data->set(idx, val); }
  size_t size() { return _data->count(); }

  /** Subclasses should redefine */
  bool data_equals(Object  * other) {
    DoubleColumn* cast = dynamic_cast<DoubleColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    DoubleColumn* clone = new DoubleColumn();
    clone->_data = _data->clone();
    return clone;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds primitive int values, unwrapped.
 */
class BoolColumn : public Column {
 public:
  PrimitiveArray<bool>* _data;

  BoolColumn() { _data = new PrimitiveArray<bool>(BOOL_CHUNK_SIZE); }

  // taking in ints in 1s and 0s, any other number is invalid
  BoolColumn(int n, ...) : BoolColumn() {
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i)
    {
      int v = va_arg(args, int);
      if(v == 1) _data->push_back(true);
      else if(v == 0) _data->push_back(false);
      else assert(false);
    }
    va_end(args);
  }

  ~BoolColumn() {
    delete(_data);
  }

  void push_back(bool val) { this->as_bool()->_data->push_back(val); }  

  bool get(size_t idx) { return _data->get(idx); }
  BoolColumn* as_bool() { return dynamic_cast<BoolColumn *>(this); }
  void set(size_t idx, bool val) { _data->set(idx, val); }
  size_t size() { return _data->count(); }

  bool data_equals(Object  * other) {
    BoolColumn* cast = dynamic_cast<BoolColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    BoolColumn* clone = new BoolColumn();
    clone->_data = _data->clone();
    return clone;
  }
};
 
/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column {
 public:
  StringArray* _data;

  StringColumn() { _data = new StringArray(STRING_CHUNK_SIZE); }

  StringColumn(int n, ...) {
    _data = new StringArray(STRING_CHUNK_SIZE);
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i) {
      String* s = new String(*va_arg(args, String *));
      _data->push_back(s);
      delete(s);
    }
    va_end(args);
  }

  ~StringColumn() {
    delete(_data);
  }

  StringColumn* as_string() { return dynamic_cast<StringColumn *>(this); }
  /** Returns the string at idx; undefined on invalid idx.*/
  String* get(size_t idx) { return _data->get(idx); }
  /** Acquire ownership for the string. */
  void set(size_t idx, String* val) { _data->set(idx, new String(*val)); }
  void push_back(String* val) { _data ->push_back(val); }
  size_t size() { return _data->count(); }

  bool data_equals(Object  * other) {
    StringColumn* cast = dynamic_cast<StringColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    StringColumn* clone = new StringColumn();
    clone->_data = _data->clone();
    return clone;
  }
};