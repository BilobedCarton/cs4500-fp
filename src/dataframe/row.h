#pragma once

#include "../utils/object.h"
#include "../utils/array.h"

#include "column.h"
#include "schema.h"

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
  virtual void accept(double f) { }
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

    ~Row() {
        for (int i = 0; i < _cols->count(); ++i)
        {
            delete(_cols->get(i));
        }
        delete(_cols);
    }

    Column* get_column_obj(size_t idx) {
        return dynamic_cast<Column *>(_cols->get(idx));
    }

    /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
    void set(size_t col, int val) { get_column_obj(col)->as_int()->set(0, val); }
    void set(size_t col, double val) { get_column_obj(col)->as_double()->set(0, val); }
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
    double get_double(size_t col) { return get_column_obj(col)->as_double()->get(0); }
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
                f.accept(get_double(i));
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