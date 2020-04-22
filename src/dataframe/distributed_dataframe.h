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

class Visitor {

};

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

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *schema_;
  }

  int get_int(size_t col, size_t row) { }
  double get_double(size_t col, size_t row) { }
  bool get_bool(size_t col, size_t row) { }
  String* get_string(size_t col, size_t row) { }

  /**
   * @brief Builds a dataframe from a visitor
   * 
   * @param key - the key this df is to be stored under
   * @param store - the store/network this df is to be stored in
   * @param sch_str - the schema string for this df
   * @param v - the visitor used to build this df
   * @return DistributedDataFrame* - the new df
   */
  static DistributedDataFrame* fromVisitor(Key* key, KVStore* store, char* sch_str, Visitor* v) {

  }

  /**
   * @brief builds a df from an array of doubles
   * 
   * @param k  - the key this df is to be stored under
   * @param store - the store/network this df is to be stored in
   * @param sz - the size of the array
   * @param arr - the array used to build this df
   * @return DistributedDataFrame* - the new df
   */
  static DistributedDataFrame* fromArray(Key* k, KVStore* store, size_t sz, double* arr) {

  }

  /**
   * @brief builds a df from a single double
   * 
   * @param k - the key this df is to be stored under
   * @param store - the store/network this df is to be stored in
   * @param scalar - the double used to build this df
   * @return DistributedDataFrame* - the new df
   */
  static DistributedDataFrame* fromScalar(Key* k, KVStore* store, double scalar) {

  }

  static DistributedDataFrame* deserialize(SerialString* ss) {

  }

  static DistributedDataFrame* deserialize(SerialString* ss, KVStore* store) {
    DistributedDataFrame* ddf = DistributedDataFrame::deserialize(ss);
    ddf->store_ = store;
  }
};