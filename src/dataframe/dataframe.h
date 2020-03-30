#pragma once

#include <assert.h>
#include <stdarg.h>

#include "../utils/helper.h"
#include "../utils/array.h"
#include "../utils/string.h"
#include "../utils/thread.h"
#include "schema.h"
#include "column.h"
#include "row.h"
 
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
          add_column(new DoubleColumn(), df.get_schema().col_name(i));
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
  double get_double(size_t col, size_t row) { return get_column_obj(col)->as_double()->get(row); }
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
    _schema->add_row(nullptr);
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


