#pragma once
#include <assert.h>

#include <stdlib.h>
#include <stdio.h>

#include "modified_dataframe.h"

static const int READ_ROW_SUCCESS = 1;
static const int READ_ROW_EOF_FAIL = -1;

/**
 * @brief      Builder to create frame objects step by step.
 */
class FrameBuilder {
public:
	FILE* _f;
	DataFrame* _df;

	/**
	 * @brief      Constructs a new instance.
	 *
	 * @param      path  The file path to build from
	 * @param      s     The schema of the file
	 */
	FrameBuilder(char* path, Schema& s) {
		_f = fopen(path, "r");
		assert(_f != nullptr);

		_df = new DataFrame(s);
	}

	/**
	 * @brief      Constructs a new instance.
	 *
	 * @param      path  The file path to build from
	 * @param      s     The schema of the file
	 */
	FrameBuilder(const char* path, Schema& s) {
		_f = fopen(path, "r");
		assert(_f != nullptr);

		_df = new DataFrame(s);
	}

	/**
	 * @brief      Destroys the object.
	 */
	~FrameBuilder() {
		fclose(_f);
	}

	// @brief      adds the given string to the given row as the corresponding type, invalid
	// 			   string input is undefined.
	//
	// @param      s     the given string
	// @param      r     the row to put the field in
	// @param[in]  idx   The index in the row to place the field
	//
	void add_field_to_row(String* s, Row& r, size_t idx) {
		switch(r.col_type(idx)) {
			case 'I':
				r.set(idx, atoi(s->c_str()));
				return;
			case 'F':
				r.set(idx, (float)atof(s->c_str()));
				return;
			case 'B':
				if(strcmp(s->c_str(), "1") == 0) r.set(idx, true);
				else if(strcmp(s->c_str(), "0") == 0) r.set(idx, false);
				else assert(false);
				return;
			case 'S':
				r.set(idx, s);
				return;
			default:
				assert(false);
				return;
		}
	}

	/**
	 * @brief      Reads a row. Returns a success/fail code.
	 *
	 * @return     1 if reading the row was successful and the row was correctly added.
	 *			   -1 if reading the row was unsuccessful 
	 */
	virtual int read_row() { return -1; }

	/**
	 * @brief      Builds the dataframe
	 *
	 * @param[in]  num_rows  The number of rows to read into the dataframe
	 *
	 * @return     the dataframe created from the given rows in the file
	 */
	DataFrame* build(size_t num_rows) {
		assert(_df != nullptr); // crash if we've already built the dataframe
		for (int i = 0; i < num_rows; ++i)
		{
			if(read_row() == -1) return _df; // end if we've reached end of file early
		}
		DataFrame* df = _df;
		_df = nullptr;
		return df;
	}
};

/**
 * @brief      Builder to create frame objects from a csv file step by step.
 */
class CSV_FrameBuilder : public FrameBuilder {
public:
	CSV_FrameBuilder(char* path, Schema& s, bool discardFirstRow) : FrameBuilder(path, s) {
		if(discardFirstRow) {
			char c = fgetc(_f);
			while(c != '\n' && c != EOF) c = fgetc(_f);
		}
	}

	CSV_FrameBuilder(const char* path, Schema& s, bool discardFirstRow) : FrameBuilder(path, s) {
		if(discardFirstRow) {
			char c = fgetc(_f);
			while(c != '\n' && c != EOF) c = fgetc(_f);
		}
	}

	~CSV_FrameBuilder() { }

	int read_row() {
		char c = fgetc(_f);
		if(c == EOF) return READ_ROW_EOF_FAIL; // fail (found end of file)

		Row* r = new Row(_df->get_schema());
		StrBuff* field = new StrBuff();
		size_t idx = 0; // current location in the row.
		// loop to end of file or end of row
		while(c != EOF && c != '\n') {
			if(c == ',') {
				// we found a comma, so we're at the end of a field
				add_field_to_row(field->get(), *r, idx++);
				delete(field);
				field = new StrBuff();
				c = fgetc(_f);
				continue;
			}
			field->addc(c);
			c = fgetc(_f);
		}
		add_field_to_row(field->get(), *r, idx);
		delete(field);
		_df->add_row(*r);
		return READ_ROW_SUCCESS; // success
	}
};