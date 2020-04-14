#pragma once

#include "../utils/object.h"
#include "../utils/string.h"
#include "../utils/primitivearray.h"
#include "../utils/args.h"

#include "../store/kvstore.h"

// page size divided by type size
#define CHUNK_MEMORY 4096

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
    PrimitiveArrayChunk<T> last_chunk_; // owned
    size_t next_node_; // where the next chunk will be shipped when completed

    /** Create an empty DistributedColumn representing the provided index */
    DistributedColumn(size_t idx) {
        idx_ = idx;
        keys_ = new Array();
        chunk_size_ = CHUNK_MEMORY / sizeof(T);
        last_chunk_ = new PrimitiveArrayChunk<T>(chunk_size_);
        next_node_ = 0;
    }
    /** Create an auto-generated DistributedColumn storing the provided arr data of length n */
    DistributedColumn(size_t idx, int n, T* arr, Dataframe* df) : DistributedColumn(idx) {

        for (int i; i < n; i++) {
            push_back(arr[i], df);
        }
        
    }
    /** ... */
    DistributedColumn(size_t idx, int n, ...) : DistributedColumn(idx) {
        // add the elements one by one
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
            StrBuf buf;
            buf.c(df->name_);
            buf.c("-c");
            buf.c(to_str<size_t>(idx_));
            buf.c("-cc");
            buf.c(to_str<size_t>(keys_->count()));

            String* kstr = buf.get();
            Key* k = new Key(kstr->c_str(), next_node_);
            keys_->append(k);

            delete(kstr);
            next_node_ = (next_node_ + 1) % args->num_nodes;

            Value v(last_chunk_);
            store_->put(k, &v);

            delete(last_chunk_);
            last_chunk_ = new PrimitiveArrayChunk<T>(chunk_size_);
        }
    }

    T get(size_t idx) {
        size_t chunk_idx = idx / chunk_size_;
        size_t idx_in_chunk = idx - (chunk_idx * chunk_size_);

        // check if pulling from cached chunk
        if(chunk_idx == keys_->count()) return last_chunk_->get(idx_in_chunk);

        // get our key, and grab the value from the store
        Key* k = dynamic_cast<Key *>(keys_->get(chunk_idx));
        assert(k != nullptr);
        PrimitiveArrayChunk<T>* chunk = PrimitiveArrayChunk::deserialize<T>(store_->get(k)->serialized());
        assert(chunk != nullptr);
        
        T v = chunk->get(idx_in_chunk);
        delete(chunk);
        return v;
    }

    void set(size_t idx, T val) {
        size_t chunk_idx = idx / chunk_size_;
        size_t idx_in_chunk = idx - (chunk_idx * chunk_size_);

        // check if setting cached chunk
        if(chunk_idx == keys_->count()) return last_chunk_->set(idx_in_chunk, val);

        // get our key, and grab the value from the store
        Key* k = dynamic_cast<Key *>(keys_->get(chunk_idx));
        assert(k != nullptr);
        PrimitiveArrayChunk<T>* chunk = PrimitiveArrayChunk::deserialize<T>(store_->get(k)->serialized());
        assert(chunk != nullptr);
        chunk->set(idx_in_chunk, val);

        // put the updated chunk back into the store.
        Value v(chunk);
        store->put(k, &v);
        delete(chunk);
    }

    size_t size() {
        // ? maybe we want to locally store the number of elements?
        return 0;
    }

    /** Return a copy of the object; nullptr is considered an error */
    Object* clone() { 
        DistributedColumn<T>* clone = new DistributedColumn<T>(idx_);
        clone->store_ = store_;
        delete(clone->keys_);
        clone->keys_ = keys_->clone();
        for (size_t i = 0; i < last_chunk_->count(); i++)
        {
            clone->last_chunk_->push_back(last_chunk_->get(i));
        }
        clone->next_node_ = next_node_;
        
        return clone;
    }

    /** Return a serialized version of this column's contents */
    SerialString* serialize() {
        size_t keys_size = 0;
        SerialString** keys_serials = new SerialString*[keys_->count()];
        for (size_t i = 0; i < keys_->count(); i++)
        {
            keys_serials[i] = keys->serialize();
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
        pso += sizeof(size_t);

        // keys_
        for (size_t i = 0; i < num_keys; i++)
        {
            memcpy(arr + pos, keys_serials[i]->data_, key_serials[i]->size_);
            pos += key_serials[i]->size_;
            delete(keys_serials[i]);
        }
        delete[](keys_serials);

        // last_chunk_
        memcpy(arr + pos, &last_chunk_serial->data_, last_chunk_serial->size_);
        pos += last_chunk_serial->ize_;
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
    template<class U>
    static DistributedColumn<U>* deserialize(SerialString* serialized) {
        size_t pos = 0;

        // idx_
        size_t idx;
        
        memcpy(&idx, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        DistributedColumn<U>* col = new DistributedColumn<U>(idx);

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
        col->last_chunk_ = PrimitiveArrayChunk::deserialize<U>(last_chunk_serial);
        delete(last_chunk_serial);

        // next_node_
        pos = serialized->size_ - sizeof(size_t);
        memcpy(&col->next_node_, serialized->data_ + pos, sizeof(size_t));

        return col;
    }

    /** Deserialized the provided string and set the store **/
    template<class U>
    static DistributedColumn<U>* deserialize(SerialString* serialized, KVStore store) {
        DistributedColumn<U>* col = DistributedColumn::deserialize<U>(serialized);
        col->set_store(store);
        return col;
    }
};