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
#include "row.h"
#include "visitor.h"
#include "distributed_column.h"

class ColumnMeta : public Object {
public:
  size_t idx;
  Key* key; // owned
  ChunkMeta* cached_chunk; // owned

  ColumnMeta(size_t i, Key* k) {
    idx = i;
    key = k;
    cached_chunk = nullptr;
  }

  ~ColumnMeta() {
    delete(key);
    delete(cached_chunk);
  }
};

/**
 * A Distributed DataFrame is a collection of keys which point to the columns belonging to this DataFrame.
 * On DataFrame build completion, the data is treated as read-only.
 * 
 */
class DistributedDataFrame : public SerializableObject {
public:
  Schema* schema_; // owned
  KVStore* store_; // not owned
  Array* keys_; // owned
  ColumnMeta* cached_column_;

  DistributedDataFrame(Schema& schema) {
    schema_ = new Schema(schema);
    keys_ = new Array();
    cached_column_ = nullptr;
  }

  ~DistributedDataFrame() {
    delete(schema_);
    for (size_t i = 0; i < keys_->count(); i++)
    {
      delete(keys_->get(i));
    }
    delete(keys_);
    if(cached_column_ != nullptr) delete(cached_column_);
  }

  /** Returns the dataframe's schema. Modifying the schema after a dataframe
    * has been created in undefined. */
  Schema& get_schema() {
    return *schema_;
  }

  void add_column(Key* k, char type) {
    schema_->add_column(type);
    keys_->append(k);
  }

  /**
   * @brief Get the column at the given index represented as a kvstore value
   * 
   * @param idx - the index of the column
   * @return Value* - the value representing the column
   */
  Value* get_column_value(size_t idx) {
    Key* k;

    // if we're looking at the cached column let's grab from there
    if(cached_column_ != nullptr && cached_column_->idx == idx) k = cached_column_->key;
    else k = dynamic_cast<Key*>(keys_->get(idx));
    
    assert(store_ != nullptr);
    Value* v = store_->get(k);

    // should we store the column in our cache?
    if(k->idx_ != store_->idx_) {
      if(cached_column_ != nullptr && !cached_column_->key->equals(k)) {
        store_->remove(cached_column_->key);
        delete(cached_column_);
      }

      Key* local_k = new Key(k->name_, store_->idx_);
      store_->put(local_k, v);
      cached_column_ = new ColumnMeta(idx, local_k);
    }
    
    return v;
  }

  /**
   * @brief Get the primitive value at the given position
   * 
   * @tparam T - the type of value
   * @param col - the column index
   * @param row - the row index
   * @return T - the value
   */
  template<class T>
  T get_primitive(size_t col, size_t row) {
    Value* v = get_column_value(col);
    DistributedColumn<T>* dc = DistributedColumn<T>::deserialize(v->serialized(), store_);
    // supply cached column
    if(cached_column_->cached_chunk != nullptr) dc->cached_chunk_ = new ChunkMeta(dynamic_cast<Key *>(cached_column_->cached_chunk->key->clone()), cached_column_->cached_chunk->chunk);

    T val = dc->get(row);

    // update cached column
    if(cached_column_ != nullptr && dc->cached_chunk_ != nullptr) {
      if(cached_column_->cached_chunk != nullptr && dc->cached_chunk_->chunk != cached_column_->cached_chunk->chunk) {
        delete(cached_column_->cached_chunk);
        cached_column_->cached_chunk = nullptr;
      }
      if(cached_column_->cached_chunk == nullptr) cached_column_->cached_chunk = new ChunkMeta(dynamic_cast<Key *>(dc->cached_chunk_->key->clone()), dc->cached_chunk_->chunk);
    }

    delete(v);
    delete(dc);
    return val;
  }

  /** get methods for our four primary types, primitives use get_primitive **/
  int get_int(size_t col, size_t row) { assert(schema_->col_type(col) == 'I'); return get_primitive<int>(col, row); }
  double get_double(size_t col, size_t row) { assert(schema_->col_type(col) == 'F'); return get_primitive<double>(col, row); }
  bool get_bool(size_t col, size_t row) { assert(schema_->col_type(col) == 'B'); return get_primitive<bool>(col, row); }
  
  String* get_string(size_t col, size_t row) { 
    assert(schema_->col_type(col) == 'S'); 
    Value* v = get_column_value(col);
    DistributedStringColumn* dsc = DistributedStringColumn::deserialize(v->serialized(), store_);
    String* val = new String(*dsc->get(row));
    delete(v);
    delete(dsc);
    return val;
  }

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
    Sys s;
    s.OK("fromVisitor not implemented.");
    assert(false);
    return nullptr;
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
    // set up df
    Schema sch("", k);
    DistributedDataFrame* ddf = new DistributedDataFrame(sch);
    ddf->store_ = store;

    // build column
    DistributedColumn<double> dc(0);
    dc.set_store(store);
    for (size_t i = 0; i < sz; i++)
    {
      dc.push_back(arr[i], ddf->get_schema().get_name());
    }

    // store column
    Key column_key(ddf->get_schema().build_col_key(0), k->idx_);
    Value column_value(&dc);
    store->put(&column_key, &column_value);

    // provide df with column
    ddf->add_column(&column_key, 'F');

    // store df
    Value df_value(ddf);
    store->put(k, &df_value);

    // return
    return ddf;
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
    double* arr = new double[1];
    arr[0] = scalar;
    DistributedDataFrame* ddf = DistributedDataFrame::fromArray(k, store, 1, arr);
    delete[](arr);
    return ddf;
  }

  SerialString* serialize() {
    SerialString* sch_ss = schema_->serialize();

    // build serial strings for each key
    SerialString** keys_strings = new SerialString*[keys_->count()];
    size_t sz = sch_ss->size_;
    for (size_t i = 0; i < keys_->count(); i++)
    {
      keys_strings[i] = dynamic_cast<Key *>(keys_->get(i))->serialize();
      sz += keys_strings[i]->size_;
    }

    char* arr = new char[sz];
    size_t pos = 0;

    // copy schema
    memcpy(arr + pos, sch_ss->data_, sch_ss->size_);
    pos += sch_ss->size_;
    delete(sch_ss);

    // copy each key
    for (size_t i = 0; i < keys_->count(); i++)
    {
      memcpy(arr + pos, keys_strings[i]->data_, keys_strings[i]->size_);
      pos += keys_strings[i]->size_;
      delete(keys_strings[i]);
    }
    delete[](keys_strings);
    
    SerialString* ss = new SerialString(arr, sz);
    delete[](arr);
    
    return ss;
  }

  static DistributedDataFrame* deserialize(SerialString* ss) {
    // grab schema and set up position
    Schema* sch = Schema::deserialize(ss);
    SerialString* temp = sch->serialize();
    size_t pos = temp->size_;
    delete(temp);

    // create df
    DistributedDataFrame* ddf = new DistributedDataFrame(*sch);
    delete(sch);

    for (size_t i = 0; i < ddf->get_schema().ncol; i++)
    {
      SerialString* substr = new SerialString(ss->data_ + pos, ss->size_ - pos);
      Key* k = Key::deserialize(substr);
      ddf->keys_->append(k);

      temp = k->serialize();
      pos += temp->size_;
      delete(temp);
      delete(substr);
    }
    
    return ddf;
  }

  static DistributedDataFrame* deserialize(SerialString* ss, KVStore* store) {
    DistributedDataFrame* ddf = DistributedDataFrame::deserialize(ss);
    ddf->store_ = store;
    return ddf;
  }
};