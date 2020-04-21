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

/**
 * A Distributed DataFrame is a collection of keys which point to the columns belonging to this DataFrame.
 * On DataFrame build completion, the data is treated as read-only.
 * 
 */
class DistributedDataFrame : public SerializableObject {
public:
  Schema* schema_;
  KVStore* store_; // not owned
  Array* keys_; // owned

  DistributedDataFrame() {

  }

  ~DistributedDataFrame() {

  }

    // Add an externally generated column to this DataFrame's key set.
  void add_column_key(Key* colKey, char typ) {
    // update the schema
    schema_->add_column(typ);

    // put into key array
    keys_->append(colKey);
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *schema_;
  }

};