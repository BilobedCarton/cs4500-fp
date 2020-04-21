#pragma once

#include "../utils/object.h"
#include "../utils/string.h"
#include "../utils/primitivearray.h"
#include "../utils/args.h"

#include "dataframe.h"
#include "../store/value.h"
#include "../store/kvstore.h"

// page size divided by type size
#define CHUNK_MEMORY 4096

/**
 * @brief Metadata for a chunk, used for representing locally cached chunk
 * 
 */
class ChunkMeta : public Object {
public:
    Key* key; // key in local store - owned
    size_t chunk; // chunk index

    ChunkMeta(Key* k, size_t c) {
        key = k;
        chunk = c;
    }

    ~ChunkMeta() {
        delete(key);
    }
};

template<class T>
/** 
 * A DistributedColumn is a collection of Keys which point to data belonging to this column.
 * When data is added to the column, it will be added to the currently cached chunk.
 * Full chunks are put into the KVStore and become a Key in this column's list of keys.
*/
class DistributedColumn : public SerializableObject {
public:
    size_t idx_; // which column are we
    KVStore* store_; // not owned
    Array* keys_; // owned
    size_t chunk_size_; // how many elements are stored in a chunk
    ChunkMeta* cached_chunk_; // owned - the chunk most recently accessed, only exists if we have a store
    PrimitiveArrayChunk<T>* last_chunk_; // owned - the chunk currently being filled by push_back
    size_t next_node_; // where the next chunk will be shipped when completed

    /** Create an empty DistributedColumn representing the provided index */
    DistributedColumn(size_t idx) {
        idx_ = idx;
        keys_ = new Array();
        chunk_size_ = CHUNK_MEMORY / sizeof(T);
        cached_chunk_ = nullptr;
        last_chunk_ = new PrimitiveArrayChunk<T>(chunk_size_);
        next_node_ = 0;
    }

    ~DistributedColumn() {
        delete(keys_);
        delete(last_chunk_);
    }

    /** Append a new value to this distributed column */
    void push_back(T val, DataFrame* df) {
        last_chunk_->push_back(val);
        // if this chunk is full, put into KVStore and prepare a new chunk
        if(last_chunk_->count() == chunk_size_) {
            
            // build the key
            StrBuff buf;
            if(df != nullptr) {
                // we're owned by a df, so we want to have a name relative to it
                buf.c(df->name_);
                buf.c("-c");
            } else {
                // we're not owned, so we are a standalone column
                buf.c("standalone-c");
            }
            buf.c(to_str<size_t>(idx_));
            buf.c("-cc");
            buf.c(to_str<size_t>(keys_->count()));

            // add the key, put the value, and update next_node_ and last_chunk_
            String* kstr = buf.get();
            Key* k = new Key(kstr->c_str(), next_node_);
            keys_->append(k);

            // maybe want to check if our key is already in use?
            Value v(last_chunk_);
            store_->put(k, &v);

            delete(last_chunk_);
            delete(kstr);
            next_node_ = (next_node_ + 1) % args->num_nodes;
            last_chunk_ = new PrimitiveArrayChunk<T>(chunk_size_);
        }
    }

    T get(size_t idx) {
        size_t chunk_idx = idx / chunk_size_;
        size_t idx_in_chunk = idx - (chunk_idx * chunk_size_);

        // check if pulling from last chunk
        if(chunk_idx == keys_->count()) return last_chunk_->get(idx_in_chunk);

        assert(store_ != nullptr); // at this point we need a store
        Key* k;
        if(cached_chunk_ != nullptr && cached_chunk_->chunk == chunk_idx) {
            // grab key to cached chunk
            k = cached_chunk_->key;
        } else {
            // grab key to external chunk
            k = dynamic_cast<Key *>(keys_->get(chunk_idx));
        }

        //  grab the value from the store
        assert(k != nullptr);
        Value* chunkV = store_->get(k);
        PrimitiveArrayChunk<T>* chunk = PrimitiveArrayChunk<T>::deserialize(chunkV->serialized());
        assert(chunk != nullptr);

        // cache the chunk if we haven't already
        if(k->idx_ != store_->idx_) {
            if(cached_chunk_ != nullptr) delete(cached_chunk_);
            cached_chunk_ = new ChunkMeta(new Key(k->name_, store_->idx_), chunk_idx);
            store_->put(cached_chunk_->key, chunkV);
        }
        
        T v = chunk->get(idx_in_chunk);
        delete(chunkV);
        delete(chunk);
        return v;
    }

    size_t size() {
        return (keys_->count() * chunk_size_) + last_chunk_->count();
    }

    /** Gets all chunks local to the given node **/
    PrimitiveArray<T>* get_local_chunks(size_t node) {
        Array local_keys;
        // select all keys with idx same as given value
        for (size_t i = 0; i < keys_->count(); i++)
        {
            Key* k = dynamic_cast<Key *>(keys_->get(i));
            if(k->idx_ == node) local_keys.append(k);
        }
        // build a super array to hold all the chunks
        PrimitiveArray<T>* arr = new PrimitiveArray<T>(chunk_size_, local_keys.count() + 1);
        for (; arr->chunks_ < local_keys.count(); arr->chunks_++)
        {
            Value* v = store_->get(dynamic_cast<Key *>(local_keys.get(arr->chunks_)));
            arr->data_[arr->chunks_] = PrimitiveArrayChunk<T>::deserialize(v->serialized());
            delete(v);
        }
        // grab last_chunk_ if we're the 0 node.
        if(node == 0) arr->data_[arr->chunks_++] = last_chunk_->clone();

        return arr;
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() { 
        DistributedColumn<T>* clone = new DistributedColumn<T>(idx_);
        clone->store_ = store_;
        delete(clone->keys_);
        clone->keys_ = new Array(keys_);
        for (size_t i = 0; i < last_chunk_->count(); i++)
        {
            clone->last_chunk_->push_back(last_chunk_->get(i));
        }
        clone->next_node_ = next_node_;
        
        return clone;
    }

    bool equals(Object* other) {
        DistributedColumn<T>* cast = dynamic_cast<DistributedColumn<T>*>(other);
        if(cast == nullptr) return false;
        if(idx_ != cast->idx_) return false;
        if(chunk_size_ != cast->chunk_size_) return false;
        if(next_node_ != cast->next_node_) return false;
        if(!keys_->equals(cast->keys_)) return false;
        return last_chunk_->equals(cast->last_chunk_);
    }

    /** Return a serialized version of this column's contents */
    SerialString* serialize() {
        size_t keys_size = 0;
        SerialString** keys_serials = new SerialString*[keys_->count()];
        for (size_t i = 0; i < keys_->count(); i++)
        {
            keys_serials[i] = dynamic_cast<Key *>(keys_->get(i))->serialize();
            keys_size += keys_serials[i]->size_;
        }

        SerialString* last_chunk_serial = last_chunk_->serialize();

        size_t sz = sizeof(size_t) + sizeof(size_t) + keys_size + last_chunk_serial->size_ + sizeof(size_t);
        char* arr = new char[sz];
        size_t pos = 0;

        // idx_
        memcpy(arr + pos, &idx_, sizeof(size_t));
        pos += sizeof(size_t);

        // num keys_
        size_t num_keys = keys_->count();
        memcpy(arr + pos, &num_keys, sizeof(size_t));
        pos += sizeof(size_t);

        // keys_
        for (size_t i = 0; i < num_keys; i++)
        {
            memcpy(arr + pos, keys_serials[i]->data_, keys_serials[i]->size_);
            pos += keys_serials[i]->size_;
            delete(keys_serials[i]);
        }
        delete[](keys_serials);

        // last_chunk_
        memcpy(arr + pos, last_chunk_serial->data_, last_chunk_serial->size_);
        pos += last_chunk_serial->size_;
        delete(last_chunk_serial);

        // next_node_
        memcpy(arr + pos, &next_node_, sizeof(size_t));
        pos += sizeof(size_t);

        SerialString* serial = new SerialString(arr, pos);
        delete[](arr);
        return serial;
    }

    void set_store(KVStore* store) {
        store_ = store;
    }

    /** Deserialize the provided String into a DistributedColumn object */
    static DistributedColumn<T>* deserialize(SerialString* serialized) {
        size_t pos = 0;

        // idx_
        size_t idx;
        
        memcpy(&idx, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        DistributedColumn<T>* col = new DistributedColumn<T>(idx);

        // keys_
        size_t num_keys;
        memcpy(&num_keys, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        for (size_t i = 0; i < num_keys; i++)
        {
            // just look at the remains of serialized
            SerialString* k_serial = new SerialString(serialized->data_ + pos, serialized->size_ - pos);
            Key* k = Key::deserialize(k_serial);
            delete(k_serial);
            k_serial = k->serialize();
            pos += k_serial->size_;
            delete(k_serial);
            col->keys_->append(k);
        }

        // last_chunk_
        delete(col->last_chunk_);
        SerialString* last_chunk_serial = new SerialString(serialized->data_ + pos, serialized->size_ - pos);
        col->last_chunk_ = PrimitiveArrayChunk<T>::deserialize(last_chunk_serial);
        delete(last_chunk_serial);

        // next_node_
        pos = serialized->size_ - sizeof(size_t);
        memcpy(&col->next_node_, serialized->data_ + pos, sizeof(size_t));

        return col;
    }

    /** Deserialized the provided string and set the store **/
    static DistributedColumn<T>* deserialize(SerialString* serialized, KVStore* store) {
        DistributedColumn<T>* col = DistributedColumn<T>::deserialize(serialized);
        col->set_store(store);
        return col;
    }
};