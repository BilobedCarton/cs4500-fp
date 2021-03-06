#pragma once

#include <assert.h>
#include <stdarg.h>

#include "../utils/helper.h"
#include "../utils/array.h"
#include "../utils/string.h"
#include "../utils/thread.h"

#include "../store/kvstore.h"
#include "../store/key.h"

#include "schema.h"
#include "column.h"
#include "row.h"
#include "visitor.h"

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public SerializableObject {
 public:
  Schema* _schema;
  Array* _cols;
  char* name_; // TODO: remove

 
  /** Create a data frame with the same columns as the give df but no rows */
  DataFrame(DataFrame& df) {
    _schema = new Schema();
    _cols = new Array();

    for (int i = 0; i < df.get_schema().width(); ++i)
    {
      switch(df.get_schema().col_type(i)) {
        case 'I':
          add_column(new IntColumn());
          break;
        case 'F':
          add_column(new DoubleColumn());
          break;
        case 'B':
          add_column(new BoolColumn());
          break;
        case 'S':
          add_column(new StringColumn());
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
          _cols->append(new DoubleColumn());
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
    for (int i = 0; i < ncols(); ++i)
    {
      delete(_cols->get(i));
    }
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
  void add_column(Column* col) {
    _schema->add_column(col->get_type());
    _cols->append(col->clone());
  }

  // helper to get and cast a column
  Column* get_column_obj(size_t col) { return dynamic_cast<Column *>(_cols->get(col)); } 
 
  /** Return the value at the given column and row. Accessing rows or
   *  columns out of bounds, or request the wrong type is undefined.*/
  int get_int(size_t col, size_t row) { return get_column_obj(col)->as_int()->get(row); }
  bool get_bool(size_t col, size_t row) { return get_column_obj(col)->as_bool()->get(row); }
  double get_double(size_t col, size_t row) { return get_column_obj(col)->as_double()->get(row); }
  String* get_string(size_t col, size_t row) { return get_column_obj(col)->as_string()->get(row); }
 
  // /** Return the offset of the given column name or -1 if no such col. */
  // int get_col(String& col) {
  //   int idx = _schema->col_idx(col.c_str());
  //   if(idx == _schema->ncol) return -1;
  //   return idx;
  // }
 
  // /** Return the offset of the given row name or -1 if no such row. */
  // int get_row(String& col) {
  //   int idx = _schema->row_idx(col.c_str());
  //   if(idx == _schema->nrow) return -1;
  //   return idx;
  // }
 
  /** Set the value at the given column and row to the given value.
    * If the column is not  of the right type or the indices are out of
    * bound, the result is undefined. */
  void set(size_t col, size_t row, int val) { get_column_obj(col)->as_int()->set(row, val); }
  void set(size_t col, size_t row, bool val) { get_column_obj(col)->as_bool()->set(row, val); }
  void set(size_t col, size_t row, double val) { get_column_obj(col)->as_double()->set(row, val); }
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
          row.set(i, get_double(i, idx));
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
    _schema->add_row();
    for (int i = 0; i < _schema->width(); ++i)
    {
      switch(_schema->col_type(i)) {
        case 'I':
          get_column_obj(i)->push_back(row.get_int(i));
          break;
        case 'F':
          get_column_obj(i)->push_back(row.get_double(i));
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
    for (int row = 0; row < nrows(); ++row)
    {
      for (int col = 0; col < ncols(); ++col)
      {
        p('<');
        switch(_schema->col_type(col)) {
          case 'I':
            p(get_int(col, row));
            break;
          case 'F':
            p(get_double(col, row));
            break;
          case 'B':
            p(get_bool(col, row));
            break;
          case 'S':
            p(get_string(col, row)->c_str());
            break;
          default:
            break;
        }
        p("> ");
      }
      p('\n');
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

  SerialString* serialize() {
    size_t size = 0;

    SerialString* sch_serial = _schema->serialize();
    size += sch_serial->size_;

    SerialString** col_serial = new SerialString*[ncols()];
    for (size_t i = 0; i < ncols(); i++) {
      col_serial[i] = get_column_obj(i)->serialize();
      size += col_serial[i]->size_;
    }
    
    char* serialStr = new char[size];
    size_t pos = 0;

    memcpy(serialStr, sch_serial->data_, sch_serial->size_);
    pos += sch_serial->size_;
    delete(sch_serial);

    for (size_t i = 0; i < ncols(); i++) {
      memcpy(&serialStr[pos], col_serial[i]->data_, col_serial[i]->size_);
      pos += col_serial[i]->size_;
      delete(col_serial[i]);
    }
    delete[](col_serial);

    SerialString* serial = new SerialString(serialStr, size);
    delete[](serialStr);

    return serial;
  }

  static DataFrame* deserialize(SerialString* serialized) {
    Schema empty("");
    DataFrame* df = new DataFrame(empty);

    size_t pos = 0;
    Schema* s = Schema::deserialize(serialized);
    pos += sizeof(size_t) + sizeof(size_t) + s->width();

    for (size_t i = 0; i < s->width(); i++)
    {
      Column* c;
      SerialString* serial = new SerialString(&serialized->data_[pos + 1], serialized->size_ - (pos + 1));
      switch(s->col_type(i)) {
        case 'I':
          c = IntColumn::deserialize(serial);
          break;
        case 'B':
          c = BoolColumn::deserialize(serial);
          break;
        case 'F':
          c = DoubleColumn::deserialize(serial);
          break;
        case 'S':
          c = StringColumn::deserialize(serial);
          break;
        default:
          c = nullptr;
          assert(false);
          break;
      }
      
      SerialString* temp = c->serialize();
      pos += temp->size_;
      df->add_column(c);
      delete(c);
      delete(temp);
      delete(serial);
    }
    delete(s);
    return df;
  }

  // DATAFRAME STATIC BUILDERS
  // from array of given size
  static DataFrame* fromArray(Key * k, KVStore* store, size_t size, int* arr) {
    Schema s("");
    DataFrame* df = new DataFrame(s);
    IntColumn col(size, arr);
    df->add_column(&col);

    Value v(df);
    store->put(k, &v);
    return df;
  }

  static DataFrame* fromArray(Key * k, KVStore* store, size_t size, bool* arr) {
    Schema s("");
    DataFrame* df = new DataFrame(s);
    BoolColumn col(size, arr);
    df->add_column(&col);

    Value v(df);
    store->put(k, &v);
    return df;
  }

  static DataFrame* fromArray(Key * k, KVStore* store, size_t size, double* arr) {
    Schema s("");
    DataFrame* df = new DataFrame(s);
    DoubleColumn col(size, arr);
    df->add_column(&col);
    
    Value v(df);
    store->put(k, &v);
    return df;
  }

  static DataFrame* fromArray(Key * k, KVStore* store, size_t size, String** arr) {
    Schema s("");
    DataFrame* df = new DataFrame(s);
    StringColumn col(size, arr);
    df->add_column(&col);
    
    Value v(df);
    store->put(k, &v);
    return df;
  }

  // from scalar value
  static DataFrame* fromScalar(Key* k, KVStore* store, int scalar) {
    int* arr = new int[1];
    arr[0] = scalar;
    return fromArray(k, store, 1, arr);
  }

  static DataFrame* fromScalar(Key* k, KVStore* store, bool scalar) {
    bool* arr = new bool[1];
    arr[0] = scalar;
    return fromArray(k, store, 1, arr);
  }

  static DataFrame* fromScalar(Key* k, KVStore* store, double scalar) {
    double* arr = new double[1];
    arr[0] = scalar;
    return fromArray(k, store, 1, arr);
  }

  static DataFrame* fromScalar(Key* k, KVStore* store, String* scalar) {
    String** arr = new String*[1];
    arr[0] = scalar;
    return fromArray(k, store, 1, arr);
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


