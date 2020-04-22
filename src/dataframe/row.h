#pragma once

#include "../utils/object.h"
#include "../utils/array.h"

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

union data {
    int i;
    bool b;
    double f;
    String* s;
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
    size_t idx_;
    size_t width_;
    data* data_; 
    char* types_;

    /** Build a row following a schema. */
    Row(Schema& scm) {
        idx_ = 0;
        width_ = scm.width();
        data_ = new data[width_];
        types_ = duplicate(scm.col_types);
    }

    ~Row() {
        delete[](data_);
    }

    /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type is undefined. */
    void set(size_t col, int val) { data_[col].i = val; }
    void set(size_t col, double val) { data_[col].f = val; }
    void set(size_t col, bool val) { data_[col].b = val; }
    /** Acquire ownership of the string. */
    void set(size_t col, String* val) { data_[col].s = val; }

    /** Set/get the index of this row (ie. its position in the dataframe. This is
     *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx) { idx_ = idx; }
    size_t get_idx() { return idx_; }

    /** Getters: get the value at the given column. If the column is not
    * of the requested type, the result is undefined. */
    int get_int(size_t col) { return data_[col].i; }
    bool get_bool(size_t col) { return data_[col].b; }
    double get_double(size_t col) { return data_[col].f; }
    String* get_string(size_t col) { return data_[col].s; }

    /** Number of fields in the row. */
    size_t width() { return width_; }

    /** Type of the field at the given position. An idx >= width is  undefined. */
    char col_type(size_t idx) { return types_[idx]; }

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
        if(idx_ != cast->idx_) return false;
        for (int i = 0; i < width(); ++i)
        {
            switch(col_type(i)) {
                case 'I':
                    if(get_int(i) != cast->get_int(i)) return false;
                    break;
                case 'B':
                    if(get_bool(i) != cast->get_bool(i)) return false;
                    break;
                case 'F':
                    if(get_double(i) != cast->get_double(i)) return false;
                    break;
                case 'S':
                    if(!get_string(i)->equals(cast->get_string(i))) return false;
                    break;
                default:
                    return false;
            }
        }
        return true;
    }
};