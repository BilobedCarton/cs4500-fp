#pragma once

#include <assert.h>
#include <stdarg.h>

#include "arraywrapper.h"
#include "utils/helper.h"
#include "utils/array.h"
#include "utils/string.h"
#include "utils/thread.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {
 public:
  size_t ncol;
  size_t nrow;
  char* col_types;
  Array* col_names; // do not own names
  Array* row_names; // do not own names
 
  /** Copying constructor */
  Schema(Schema& from) {
    ncol = from.ncol;
    nrow = from.nrow;
    Sys s;
    col_types = s.duplicate(from.col_types);
    col_names = new Array(from.col_names);
    row_names = new Array(from.row_names);
  }
 
  /** Create an empty schema **/
  Schema() {
    ncol = 0;
    nrow = 0;
    col_types = new char[1];
    col_types[0] = '\0';
    col_names = new Array();
    row_names = new Array();
  }
 
  /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
  Schema(const char* types) {
    Sys s;
    col_types = s.duplicate(types);
    ncol = strlen(col_types);
    nrow = 0;
    col_names = new Array();
    for (int i = 0; i < ncol; ++i)
    {
      col_names->append(nullptr);
    }
    row_names = new Array();
  }

  ~Schema() {
    delete[](col_types);
    delete(col_names);
    delete(row_names);
  }
 
  /** Add a column of the given type and name (can be nullptr), name
    * is external. */
  void add_column(char typ, String* name) {
    char* new_col_types = new char[ncol + 2];
    strcpy(new_col_types, col_types);
    delete[](col_types);
    new_col_types[ncol] = typ;
    new_col_types[++ncol] = '\0';
    col_types = new_col_types;

    if(name != nullptr) col_names->append(name->clone());
    else col_names->append(name);
  }
 
  /** Add a row with a name (possibly nullptr), name is external. */
  void add_row(String* name) {
    nrow++;
    if(name != nullptr) row_names->append(name->clone());
    else row_names->append(name);
  }
 
  /** Return name of row at idx; nullptr indicates no name. An idx >= width
    * is undefined. */
  String* row_name(size_t idx) {
    if(idx >= nrow) return nullptr;
    return dynamic_cast<String *>(row_names->get(idx));
  }
 
  /** Return name of column at idx; nullptr indicates no name given.
    *  An idx >= width is undefined.*/
  String* col_name(size_t idx) {
    if(idx >= ncol) return nullptr;
    return dynamic_cast<String *>(col_names->get(idx));
  }
 
  /** Return type of column at idx. An idx >= width is undefined. */
  char col_type(size_t idx) {
    if(idx >= ncol) return '\0';
    return col_types[idx];
  }
 
  /** Given a column name return its index, or -1. */
  int col_idx(const char* name) {
    String* finder = new String(name);
    int idx = col_names->index(finder);
    delete(finder);
    return idx;
  }
 
  /** Given a row name return its index, or -1. */
  int row_idx(const char* name) {
    String* finder = new String(name);
    int idx = row_names->index(finder);
    delete(finder);
    return idx;
  }
 
  /** The number of columns */
  size_t width() {
    return ncol;
  }
 
  /** The number of rows */
  size_t length() {
    return nrow;
  }

  bool equals(Object  * other) {
    Schema* cast = dynamic_cast<Schema *>(other);
    if(cast == nullptr) return false;
    return nrow == cast->nrow 
      && ncol == cast->ncol 
      && strcmp(col_types, cast->col_types) == 0 
      && cast->col_names->equals(col_names) 
      && cast->row_names->equals(row_names);
  }

  Object* clone() { return new Schema(*this); }
};

class IntColumn;
class StringColumn;
class BoolColumn;
class FloatColumn;
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
  virtual FloatColumn* as_float() { return nullptr; }
  virtual StringColumn* as_string() { return nullptr; }
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  virtual void push_back(int val) { return; }
  virtual void push_back(bool val) { return; }
  virtual void push_back(float val) { return; }
  virtual void push_back(String* val) { return; }
 
 /** Returns the number of elements in the column. */
  virtual size_t size() { return 0; }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
  char get_type() { 
    if(as_int() != nullptr) return 'I';
    if(as_bool() != nullptr) return 'B';
    if(as_float() != nullptr) return 'F';
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
  IntArrayWrapper* _data;

  IntColumn() { _data = new IntArrayWrapper(); }

  IntColumn(int n, ...) {
    _data = new IntArrayWrapper();
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i)
    {
      _data->append(va_arg(args, int));
    }
    va_end(args);
  }

  ~IntColumn() {
    delete(_data);
  }

  void push_back(int val) { this->as_int()->_data->append(val); }

  int get(size_t idx) { return _data->get(idx); }
  IntColumn* as_int() { return dynamic_cast<IntColumn *>(this); }
  void set(size_t idx, int val) { _data->set(idx, val); }
  size_t size() { return _data->size(); }

  bool data_equals(Object  * other) {
    IntColumn* cast = dynamic_cast<IntColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    IntColumn* clone = new IntColumn();
    for (int i = 0; i < size(); ++i)
    {
      clone->push_back(get(i));
    }
    return clone;
  }
};
 
// Other primitive column classes similar...

/*************************************************************************
 * FloatColumn::
 * Holds primitive int values, unwrapped.
 */
class FloatColumn : public Column {
 public:
  FloatArrayWrapper* _data;

  FloatColumn() { _data = new FloatArrayWrapper(); }

  FloatColumn(int n, ...) {
    _data = new FloatArrayWrapper();
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i)
    {
      _data->append(static_cast<float>(va_arg(args, double)));
    }
    va_end(args);
  }

  ~FloatColumn() {
    delete(_data);
  }

  void push_back(float val) { this->as_float()->_data->append(val); }

  float get(size_t idx) { return _data->get(idx); }
  FloatColumn* as_float() { return dynamic_cast<FloatColumn *>(this); }
  void set(size_t idx, float val) { _data->set(idx, val); }
  size_t size() { return _data->size(); }

  /** Subclasses should redefine */
  bool data_equals(Object  * other) {
    FloatColumn* cast = dynamic_cast<FloatColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    FloatColumn* clone = new FloatColumn();
    for (int i = 0; i < size(); ++i)
    {
      clone->push_back(get(i));
    }
    return clone;
  }
};

/*************************************************************************
 * BoolColumn::
 * Holds primitive int values, unwrapped.
 */
class BoolColumn : public Column {
 public:
  BoolArrayWrapper* _data;

  BoolColumn() { _data = new BoolArrayWrapper(); }

  // taking in ints in 1s and 0s, any other number is invalid
  BoolColumn(int n, ...) {
    _data = new BoolArrayWrapper();
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i)
    {
      int v = va_arg(args, int);
      if(v == 1) _data->append(true);
      else if(v == 0) _data->append(false);
      else assert(false);
    }
    va_end(args);
  }

  ~BoolColumn() {
    delete(_data);
  }

  void push_back(bool val) { this->as_bool()->_data->append(val); }  

  bool get(size_t idx) { return _data->get(idx); }
  BoolColumn* as_bool() { return dynamic_cast<BoolColumn *>(this); }
  void set(size_t idx, bool val) { _data->set(idx, val); }
  size_t size() { return _data->size(); }

  bool data_equals(Object  * other) {
    BoolColumn* cast = dynamic_cast<BoolColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    BoolColumn* clone = new BoolColumn();
    for (int i = 0; i < size(); ++i)
    {
      clone->push_back(get(i));
    }
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
  StringArrayWrapper* _data;

  StringColumn() { _data = new StringArrayWrapper(); }

  StringColumn(int n, ...) {
    _data = new StringArrayWrapper();
    va_list args;
    va_start(args, n);
    for (int i = 0; i < n; ++i) {
      _data->append(new String(*va_arg(args, String *)));
    }
    va_end(args);
  }

  ~StringColumn() {
    for (int i = 0; i < _data->size(); ++i)
    {
      delete(_data->get(i));
    }
    delete(_data);
  }

  StringColumn* as_string() { return dynamic_cast<StringColumn *>(this); }
  /** Returns the string at idx; undefined on invalid idx.*/
  String* get(size_t idx) { return _data->get(idx); }
  /** Acquire ownership for the string. */
  void set(size_t idx, String* val) { _data->set(idx, new String(*val)); }
  void push_back(String* val) { _data ->append(new String(*val)); }
  size_t size() { return _data->size(); }

  bool data_equals(Object  * other) {
    StringColumn* cast = dynamic_cast<StringColumn *>(other);
    if(cast == nullptr) return false;
    if(cast->size() != size()) return false;
    return cast->_data->equals(_data);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() { 
    StringColumn* clone = new StringColumn();
    for (int i = 0; i < size(); ++i)
    {
      clone->push_back(get(i));
    }
    return clone;
  }
};

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:
 
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) { }
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) { }
  virtual void accept(float f) { }
  virtual void accept(int i) { }
  virtual void accept(String* s) { }
 
  /** Called when all fields have been seen. */
  virtual void done() { }
};
 
/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
 public:
  size_t _idx;
  Array* _cols; 
 
  /** Build a row following a schema. */
  Row(Schema& scm) {
    _idx = 0;
    _cols = new Array();
    for (int i = 0; i < scm.width(); ++i)
    {
      switch(scm.col_type(i)) {
        case 'I':
          _cols->append(new IntColumn());
          break;
        case 'F':
          _cols->append(new FloatColumn());
          break;
        case 'B':
          _cols->append(new BoolColumn());
          break;
        case 'S':
          _cols->append(new StringColumn());
          break;
        default:
          break;
      }
    }
  }

  Column* get_column_obj(size_t idx) {
    return dynamic_cast<Column *>(_cols->get(idx));
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
  void set(size_t col, int val) { get_column_obj(col)->as_int()->set(0, val); }
  void set(size_t col, float val) { get_column_obj(col)->as_float()->set(0, val); }
  void set(size_t col, bool val) { get_column_obj(col)->as_bool()->set(0, val); }
  /** Acquire ownership of the string. */
  void set(size_t col, String* val) { get_column_obj(col)->as_string()->set(0, val); }
 
  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) { _idx = idx; }
  size_t get_idx() { return _idx; }
 
  /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
  int get_int(size_t col) { return get_column_obj(col)->as_int()->get(0); }
  bool get_bool(size_t col) { return get_column_obj(col)->as_bool()->get(0); }
  float get_float(size_t col) { return get_column_obj(col)->as_float()->get(0); }
  String* get_string(size_t col) { return get_column_obj(col)->as_string()->get(0); }
 
  /** Number of fields in the row. */
  size_t width() { return _cols->size_; }
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) { return get_column_obj(idx)->get_type(); }
 
  /** Given a Fielder, visit every field of this row.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    assert(get_idx() == idx);
    f.start(idx);
    for (int i = 0; i < width(); ++i)
    {
      switch(col_type(i)) {
        case 'I':
          f.accept(get_int(i));
          break;
        case 'F':
          f.accept(get_float(i));
          break;
        case 'B':
          f.accept(get_bool(i));
          break;
        case 'S':
          f.accept(get_string(i));
          break;
        default:
          break;
      }
    }
    f.done();
  }

  // determines if the given object stores the same data as this row. 
  // helper for personal tests
  bool data_equals(Object  * other) {
    Row* cast = dynamic_cast<Row *>(other);
    if(cast == nullptr) return false;
    if(_idx != cast->_idx) return false;
    for (int i = 0; i < width(); ++i)
    {
      if(!get_column_obj(i)->data_equals(cast->get_column_obj(i))) return false;
    }
    return true;
  }
};
 
/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object {
 public:
  /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
  virtual bool accept(Row& r) { return true; }
 
  /** Once traversal of the data frame is complete the rowers that were
      split off will be joined.  There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
  virtual void join_delete(Rower* other) { delete(other); }

  // clones this type of rower.
  virtual Object * clone() { return new Rower(); }
};

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {
 public:
  Schema* _schema;
  Array* _cols;
 
  /** Create a data frame with the same columns as the give df but no rows */
  DataFrame(DataFrame& df) {
    _schema = new Schema();
    _cols = new Array();
    for (int i = 0; i < df.get_schema().width(); ++i)
    {
      switch(df.get_schema().col_type(i)) {
        case 'I':
          add_column(new IntColumn(), df.get_schema().col_name(i));
          break;
        case 'F':
          add_column(new FloatColumn(), df.get_schema().col_name(i));
          break;
        case 'B':
          add_column(new BoolColumn(), df.get_schema().col_name(i));
          break;
        case 'S':
          add_column(new StringColumn(), df.get_schema().col_name(i));
          break;
        default:
          break;
      }
    }
  }
 
  /** Create a data frame from a schema and columns. Results are undefined if
    * the columns do not match the schema. */
  DataFrame(Schema& schema) {
    _schema = new Schema(schema);
    _cols = new Array();
    for (int i = 0; i < _schema->width(); ++i)
    {
      switch(_schema->col_type(i)) {
        case 'I':
          _cols->append(new IntColumn());
          break;
        case 'F':
          _cols->append(new FloatColumn());
          break;
        case 'B':
          _cols->append(new BoolColumn());
          break;
        case 'S':
          _cols->append(new StringColumn());
          break;
        default:
          break;
      }
    }
  }

  ~DataFrame() {
    delete(_schema);
    delete(_cols);
  }
 
  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *_schema;
  }
 
  /** Adds a column this dataframe, updates the schema, the new column
    * is external, and appears as the last column of the dataframe, the
    * name is optional and external. A nullptr colum is undefined. */
  void add_column(Column* col, String* name) {
    _schema->add_column(col->get_type(), name);
    _cols->append(col->clone());
  }

  // helper to get and cast a column
  Column* get_column_obj(size_t col) { return dynamic_cast<Column *>(_cols->get(col)); } 
 
  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) { return get_column_obj(col)->as_int()->get(row); }
  bool get_bool(size_t col, size_t row) { return get_column_obj(col)->as_bool()->get(row); }
  float get_float(size_t col, size_t row) { return get_column_obj(col)->as_float()->get(row); }
  String* get_string(size_t col, size_t row) { return get_column_obj(col)->as_string()->get(row); }
 
  /** Return the offset of the given column name or -1 if no such col. */
  int get_col(String& col) {
    int idx = _schema->col_idx(col.c_str());
    if(idx == _schema->ncol) return -1;
    return idx;
  }
 
  /** Return the offset of the given row name or -1 if no such row. */
  int get_row(String& col) {
    int idx = _schema->row_idx(col.c_str());
    if(idx == _schema->nrow) return -1;
    return idx;
  }
 
  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val) { get_column_obj(col)->as_int()->set(row, val); }
  void set(size_t col, size_t row, bool val) { get_column_obj(col)->as_bool()->set(row, val); }
  void set(size_t col, size_t row, float val) { get_column_obj(col)->as_float()->set(row, val); }
  void set(size_t col, size_t row, String val) { get_column_obj(col)->as_string()->set(row, &val); }
 
  /** Set the fields of the given row object with values from the columns at
    * the given offset.  If the row is not form the same schema as the
    * dataframe, results are undefined.
    */
  void fill_row(size_t idx, Row& row) {
    row.set_idx(idx);
    for (int i = 0; i < _schema->width(); ++i)
    {
      switch(_schema->col_type(i)) {
        case 'I':
          row.set(i, get_int(i, idx));
          break;
        case 'F':
          row.set(i, get_float(i, idx));
          break;
        case 'B':
          row.set(i, get_bool(i, idx));
          break;
        case 'S':
          row.set(i, new String(*get_string(i, idx)));
          break;
        default:
          break;
      }
    }
  }
 
  /** Add a row at the end of this dataframe. The row is expected to have
   *  the right schema and be filled with values, otherwise undedined.  */
  void add_row(Row& row) {
    _schema->add_row(nullptr);
    for (int i = 0; i < _schema->width(); ++i)
    {
      switch(_schema->col_type(i)) {
        case 'I':
          get_column_obj(i)->push_back(row.get_int(i));
          break;
        case 'F':
          get_column_obj(i)->push_back(row.get_float(i));
          break;
        case 'B':
          get_column_obj(i)->push_back(row.get_bool(i));
          break;
        case 'S':
          get_column_obj(i)->push_back(row.get_string(i));
          break;
        default:
          break;
      }
    }
  }
 
  /** The number of rows in the dataframe. */
  size_t nrows() { return _schema->nrow; }
 
  /** The number of columns in the dataframe.*/
  size_t ncols() { return _schema->ncol; }
 
  /** Visit rows in order */
  void map(Rower& r) { 
    Row row(get_schema());
    for (int i = 0; i < nrows(); ++i)
    {
      fill_row(i, row);
      r.accept(row);
    }
  }

  /** This method clones the Rower and executes the map in parallel. Join is
  * used at the end to merge the results. */
  void pmap(Rower& r);
 
  /** Create a new dataframe, constructed from rows for which the given Rower
    * returned true from its accept method. */
  DataFrame* filter(Rower& r) { 
    DataFrame* df = new DataFrame(*this);
    for (int i = 0; i < nrows(); ++i)
    {
      Row* row = new Row(*_schema);
      fill_row(i, *row);
      if(r.accept(*row)) {
        df->add_row(*row);
      }
      delete(row);
    }
    return df;
  }
 
  /** Print the dataframe in SoR format to standard output. */
  void print() {
    Sys s;
    for (int row = 0; row < nrows(); ++row)
    {
      for (int col = 0; col < ncols(); ++col)
      {
        s.p('<');
        switch(_schema->col_type(col)) {
          case 'I':
            s.p(get_int(col, row));
            break;
          case 'F':
            s.p(get_float(col, row));
            break;
          case 'B':
            s.p(get_bool(col, row));
            break;
          case 'S':
            s.p(get_string(col, row)->c_str());
            break;
          default:
            break;
        }
        s.p("> ");
      }
      s.p('\n');
    }
  }

  // check if the stored data is identical to the given dataframe
  bool data_equals(Object * other) {
    DataFrame* cast = dynamic_cast<DataFrame *>(other);
    if(cast == nullptr) return false;
    if(!_schema->equals(cast->_schema)) return false;
    for (int i = 0; i < ncols(); ++i)
    {
      if(!get_column_obj(i)->data_equals(cast->get_column_obj(i))) return false;
    }
    return true;
  }

  // only check if schema and column pointers are identical, we can use data_equals for real equality of stored information
  bool equals(Object  * other) { 
    DataFrame* cast = dynamic_cast<DataFrame *>(other);
    if(cast == nullptr) return false;
    return _schema->equals(cast->_schema) && _cols->equals(cast->_cols);
  }

  /** Return a copy of the object; nullptr is considered an error */
  Object* clone() {
    Rower r;
    return filter(r);
  }
};

/**
 * @brief      This class describes a threadable rower that maintains its own thread to row ratio.
 *             This allows the program to optimize the number of threads to spawn.
 */
class ThreadableRower : public Rower {
  size_t _thread_to_row_ratio;

  /**
   * @brief      Constructs a new instance.
   *
   * @param[in]  ratio  The ratio of threads to rows that is optimal for this rower
   */
  ThreadableRower(size_t ratio) { _thread_to_row_ratio = ratio; }

  /**
   * @brief      Gets the ratio of threads to rows.
   *
   * @return     The ratio of threads of rows.
   */
  size_t get_ratio() { return _thread_to_row_ratio; }
};

/****************************************************************************
 * RowerThread::
 * 
 * A RowerThread is a thread runner that runs a rower on a given set of rows.
 * If the set contains more than one row, the RowerThread clones the rower and sets up 2 child RowerThreads.
 * It then splits up the set evenly between the two threads.
 * The set of rows is determined by given start and end row indices (where start is inclusive, and end is not).
 */
class RowerThread : public Thread {
public:
  DataFrame* _df;
  Rower* _p;
  size_t _start;
  size_t _end; // exclusive

  /**
   * @brief      Constructs a new instance.
   *
   * @param      df     the dataframe
   * @param      p      the rower to be executed
   * @param[in]  start  The starting row (inclusive)
   * @param[in]  end    The ending row (exclusive)
   */
  RowerThread(DataFrame* df, Rower* p, size_t start, size_t end) {
    _df = df;
    _p = dynamic_cast<Rower *>(p->clone());
    _start = start;
    _end = end;
  }

  virtual void run() { 
    // execute on our assigned row
    Row r(_df->get_schema());
    for (int i = _start; i < _end; ++i)
    {
      _df->fill_row(i, r);
      _p->accept(r);
    }
  }
};

const size_t THREAD_ROW_RATIO = 100000; // number of rows to 1 thread

/** This method clones the Rower and executes the map in parallel. Join is
* used at the end to merge the results. */
void DataFrame::pmap(Rower& r) {
  size_t numThreadsToSpawn = ((nrows() - (nrows() % THREAD_ROW_RATIO)) / THREAD_ROW_RATIO) + 1;
  
  if(numThreadsToSpawn <= 1) {
    // we don't have enough rows to need multithreading
    map(r);
    return;
  }

  // distribute rows evenly between threads
  size_t rowsPerThread = nrows() / numThreadsToSpawn;
  RowerThread** threads = new RowerThread*[numThreadsToSpawn];

  // spawn the threads
  for (int i = 0; i < numThreadsToSpawn; ++i)
  {
    size_t end = i == numThreadsToSpawn - 1 ? nrows() : (i + 1) * rowsPerThread;
    threads[i] = new RowerThread(this, &r, i * rowsPerThread, end);
    threads[i]->start();
  }
  // await the threads' completion and join them to the main rower
  for (int i = 0; i < numThreadsToSpawn; ++i)
  {
    threads[i]->join();
    r.join_delete(threads[i]->_p);
  }
}


