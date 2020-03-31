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
    size_t ncol;
    size_t nrow;
    char* col_types;
    // Array* col_names; // do not own names
    // Array* row_names; // do not own names

    /** Copying constructor */
    Schema(Schema& from) {
        ncol = from.ncol;
        nrow = from.nrow;
        col_types = duplicate(from.col_types);
        // col_names = new Array(from.col_names);
        // row_names = new Array(from.row_names);
    }

    /** Create an empty schema **/
    Schema() {
        ncol = 0;
        nrow = 0;
        col_types = new char[1];
        col_types[0] = '\0';
        // col_names = new Array();
        // row_names = new Array();
    }

    /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr argument is
    * undefined. **/
    Schema(const char* types) {
        col_types = duplicate(types);
        ncol = strlen(col_types);
        nrow = 0;
        // col_names = new Array();
        // for (int i = 0; i < ncol; ++i)
        // {
        //     col_names->append(nullptr);
        // }
        // row_names = new Array();
    }

    ~Schema() {
        delete[](col_types);
        // delete(col_names);
        // delete(row_names);
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

        // if(name != nullptr) col_names->append(name->clone());
        // else col_names->append(name);
    }

    /** Add a row with a name (possibly nullptr), name is external. */
    void add_row() {
        nrow++;
        // if(name != nullptr) row_names->append(name->clone());
        // else row_names->append(name);
    }

    // /** Return name of row at idx; nullptr indicates no name. An idx >= width
    // * is undefined. */
    // String* row_name(size_t idx) {
    //     if(idx >= nrow) return nullptr;
    //     return dynamic_cast<String *>(row_names->get(idx));
    // }

    // /** Return name of column at idx; nullptr indicates no name given.
    // *  An idx >= width is undefined.*/
    // String* col_name(size_t idx) {
    //     if(idx >= ncol) return nullptr;
    //     return dynamic_cast<String *>(col_names->get(idx));
    // }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        if(idx >= ncol) return '\0';
        return col_types[idx];
    }

    // /** Given a column name return its index, or -1. */
    // int col_idx(const char* name) {
    //     String* finder = new String(name);
    //     int idx = col_names->index(finder);
    //     delete(finder);
    //     return idx;
    // }

    // /** Given a row name return its index, or -1. */
    // int row_idx(const char* name) {
    //     String* finder = new String(name);
    //     int idx = row_names->index(finder);
    //     delete(finder);
    //     return idx;
    // }

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
            // && cast->col_names->equals(col_names) 
            // && cast->row_names->equals(row_names);
    }

    Object* clone() { return new Schema(*this); }

    SerialString* serialize() {
        char* serial = new char[sizeof(size_t) + sizeof(size_t) + ncol];
        size_t pos = 0;

        memcpy(&serial[pos], &ncol, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(&serial[pos], &nrow, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(&serial[pos], col_types, ncol);
        
        SerialString* s = new SerialString(serial, sizeof(size_t) + sizeof(size_t) + ncol);
        delete[](serial);
        return s;
    }

    static Schema* deserialize(SerialString* serialized) {
        size_t col;
        size_t row;
        size_t pos = 0;
        
        memcpy(&col, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(&row, &serialized->data_[pos], sizeof(size_t));
        pos += sizeof(size_t);

        char* types = new char[col + 1];
        memcpy(types, &serialized->data_[pos], col);
        types[col] = '\0';

        Schema* s = new Schema(types);
        delete[](types);
        s->nrow = row;
        return s;
    }
};