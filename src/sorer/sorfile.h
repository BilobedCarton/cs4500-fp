//lang::CwC

#pragma once

#include "field.h"
#include "object.h"
#include "string.h"
#include "helper.h"

/**
 * @brief      This class describes a SoR file.
 */
class SorFile : public Object {
public:
	size_t schema_size_;
	size_t* schema_; // owned
	List* cols_; // owned

	/**
	 * @brief      Constructs a new instance.
	 *
	 * @param      schema  The schema of the SoR file.
	 */
	SorFile(size_t* schema, size_t schema_size) : Object() {
		schema_size_ = schema_size;
		schema_ = new size_t[schema_size];
		memcpy(schema_, schema, schema_size * sizeof(size_t));
		cols_ = new List();

		for (int i = 0; i < schema_size; ++i) {
			add_col();
		}
	}

	/**
	 * @brief      Destroys the object.
	 */
	~SorFile() {
		for (int i = 0; i < schema_size_; ++i)
		{
			List* col = dynamic_cast<List *>(cols_->get(i));
			for (int j = 0; j < col->size_; ++j)
			{
				delete(col->get(j));
			}
			delete(col);
		}
		delete[](schema_);
		delete(cols_);
	}

	/**
	 * @brief      Returns a character array representation of the object.
	 *
	 * @return     Character array representation of the object.
	 */
	char* to_char_arr() {
		GrowableString* gs = new GrowableString("");
		for (int i = 0; i < schema_size_; ++i)
		{
			gs->add_char('<');
			switch(schema_[i]) {
				case INT_TYPE:
					gs->add_char('I');
					break;
				case FLOAT_TYPE:
					gs->add_char('F');
					break;
				case STRING_TYPE:
					gs->add_char('S');
					break;
				case BOOL_TYPE:
					gs->add_char('B');
					break;
				default:
					gs->add_char('X');
			}
			gs->add_char('>');
		}
		gs->add_char('\n');
		// now add rows
		size_t longest_col = 0;
		for (int i = 0; i < schema_size_; ++i)
		{
			List* col = dynamic_cast<List *>(cols_->get(i));
			if(col->size_ > longest_col) longest_col = col->size_;
		}
		for (int i = 0; i < longest_col; ++i)
		{
			for (int j = 0; j < schema_size_; ++j)
			{	
				if(j == 0) { 
					gs->append(int_to_string(i+1));
					gs->append(": ");
				}
				Field* f = get_field(j, i);
				if(f == nullptr) f = new Field(MISSING_TYPE, NULL);
				char* fStr = f->to_char_arr();
				gs->append(fStr);
				delete[](fStr);
			}
			gs->add_char('\n');
		}
		return gs->str_;
	}

	/**
	 * @brief      Gets the column type.
	 *
	 * @param[in]  colIdx  The column index
	 *
	 * @return     The column type.
	 */
	size_t get_col_type(size_t colIdx) {
		assert(colIdx < schema_size_);
		return schema_[colIdx];
	}

	/**
	 * @brief      Adds a column.
	 */
	void add_col() {
		cols_->push_back(new List());
	}

	/**
	 * @brief      Adds a field.
	 *
	 * @param[in]  colIdx  The column index
	 * @param      field   The field
	 */
	void add_field(size_t colIdx, Field* field) {
		dynamic_cast<List*>(cols_->get(colIdx))->push_back(field);
	}

	/**
	 * @brief      Inserts a field at the given position.
	 *
	 * @param[in]  colIdx  The column index
	 * @param[in]  rowIdx  The row index
	 * @param      field   The field
	 */
	void insert_field(size_t colIdx, size_t rowIdx, Field* field) {
		dynamic_cast<List*>(cols_->get(colIdx))->add(rowIdx, field);
	}

	/**
	 * @brief      Gets the field at the given position.
	 *
	 * @param[in]  colIdx  The column index
	 * @param[in]  rowIdx  The row index
	 *
	 * @return     The field.
	 */
	Field* get_field(size_t colIdx, size_t rowIdx) {
		if(colIdx >= schema_size_) return nullptr;
		List* col = dynamic_cast<List *>(cols_->get(colIdx));
		if(rowIdx >= col->size_) return nullptr;
		return dynamic_cast<Field *>(col->get(rowIdx));
	}

	void discard_row(size_t rowIdx) {
		for (int i = 0; i < schema_size_; ++i)
		{
			List* col = dynamic_cast<List *>(cols_->get(i));
			if(col->size_ > rowIdx) col->remove(rowIdx);
		}
	}
};