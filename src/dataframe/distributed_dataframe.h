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
#include "distributed_column.h"

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

/**
 * A Distributed DataFrame is a collection of keys which point to the columns belonging to this DataFrame.
 * On DataFrame build completion, the data is treated as read-only.
 * 
 */
class DistributedDataFrame : public SerializableObject {
public:
    Schema* _schema;
    KVStore* store_; // not owned
    Array* keys_; // owned

    DistributedDataFrame() {

    }

    ~DistributedDataFrame() {

    }


    // Add an externally generated column to this DataFrame's key set.
  void add_column_key(Key* colKey) {
    // update the schema

    // put into key array
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *_schema;
  }

};