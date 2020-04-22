#pragma once

#include "../utils/object.h"
#include "../utils/string.h"
#include "../utils/array.h"

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public SerializableObject {
public:
    String* name; // owned
    size_t ncol;
    size_t nrow;
    char* col_types; // owned

    /** Copying constructor */
    Schema(Schema& from) {
        name = new String(*from.name);
        ncol = from.ncol;
        nrow = from.nrow;
        col_types = duplicate(from.col_types);
    }

    /** Create an empty schema **/
    Schema() {
        name = new String("standaloneDF");
        ncol = 0;
        nrow = 0;
        col_types = new char[1];
        col_types[0] = '\0';
    }

    /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
    Schema(const char* types) {
        name = new String("standaloneDF");
        col_types = duplicate(types);
        ncol = strlen(col_types);
        nrow = 0;
    }

    
    Schema(const char* types, Key* k) : Schema(types) {
        delete(name);
        name = new String(k->name_);
    }

    ~Schema() {
        delete[](col_types);
    }

    /** Add a column of the given type and name (can be nullptr), name
    * is external. */
    void add_column(char typ) {
        char* new_col_types = new char[ncol + 2];
        strcpy(new_col_types, col_types);
        delete[](col_types);
        new_col_types[ncol] = typ;
        new_col_types[++ncol] = '\0';
        col_types = new_col_types;
    }

    /** Add a row with a name (possibly nullptr), name is external. */
    void add_row() {
        nrow++;
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        if(idx >= ncol) return '\0';
        return col_types[idx];
    }

    /** get the name of this dataframe **/
    String* get_name() {
        return name;
    }

    /**
     * @brief Constructs a char* to be used as the given column's key
     * 
     * @param idx - the index of the column
     * @return char* - the key to use for that column
     */
    char* build_col_key(size_t idx) {
        StrBuff buf;

        // use our name
        buf.c(name->c_str());
        buf.c("-c");

        // use the column's index
        char* idx_str = to_str<size_t>(idx);
        buf.c(idx_str);
        delete[](idx_str);

        // grab, duplicate, delete, and return
        String* str = buf.get();
        char* key = duplicate(str->c_str());
        delete(str);
        return key;
    }

    /** The number of columns */
    size_t width() {
        return ncol;
    }

    /** The number of rows */
    size_t length() {
        return nrow;
    }

    bool equals(Object  * other) {
        Schema* cast = dynamic_cast<Schema *>(other);
        if(cast == nullptr) return false;
        return nrow == cast->nrow 
            && ncol == cast->ncol 
            && strcmp(col_types, cast->col_types) == 0;
    }

    Object* clone() { return new Schema(*this); }

    SerialString* serialize() {
        SerialString* name_ss = name->serialize();

        char* serial = new char[name_ss->size_ + sizeof(size_t) + sizeof(size_t) + ncol];
        size_t pos = 0;

        // name
        memcpy(&serial[pos], name_ss->data_, name_ss->size_);
        pos += name_ss->size_;
        delete(name_ss);

        // ncol
        memcpy(&serial[pos], &ncol, sizeof(size_t));
        pos += sizeof(size_t);

        // nrow
        memcpy(&serial[pos], &nrow, sizeof(size_t));
        pos += sizeof(size_t);

        // col_types
        memcpy(&serial[pos], col_types, ncol);
        pos += ncol;
        
        SerialString* s = new SerialString(serial, pos);
        delete[](serial);
        return s;
    }

    static Schema* deserialize(SerialString* serialized) {
        String* name;
        size_t col;
        size_t row;
        size_t pos = 0;

        name = String::deserialize(serialized);
        pos += sizeof(size_t) + name->size();
        
        memcpy(&col, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(&row, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        char* types = new char[col + 1];
        memcpy(types, &serialized->data_[pos], col);
        types[col] = '\0';

        Schema* s = new Schema(types);
        delete[](types);
        delete(s->name);
        s->name = name;
        s->nrow = row;
        return s;
    }
};