#pragma once
#include <assert.h>

#include <stdlib.h>
#include <stdio.h>

#include "dataframe.h"
#include "../utils/helper.h"
#include "../utils/string.h"

static const int READ_ROW_SUCCESS = 1;
static const int READ_ROW_EOF_FAIL = -1;

class SOR_SchemaBuilder {
public:
	FILE* _f;

	SOR_SchemaBuilder(char* path) {
		_f = fopen(path, "r");
		assert(_f != nullptr);
	}

	~SOR_SchemaBuilder() {
		fclose(_f);
	}

	char classify_field(String* field) {
		if(field->size() == 0) return 'M'; // missing

		char* validity_check = to_str<float>((float)atof(field->c_str()));
		if(strcmp(validity_check, field->c_str()) == 0) {
			delete[](validity_check);
			return 'F'; // float parsing was valid
		}

		delete[](validity_check);
		validity_check = to_str<int>(atoi(field->c_str()));
		if(strcmp(validity_check, field->c_str()) == 0) { // int parsing was valid
			delete[](validity_check);
			// could be int or bool
			int val = atoi(field->c_str());
			if(val == 0 || val == 1) return 'B';
			return 'I';
		}
		delete[](validity_check);

		// otherwise it's a string.
		return 'S';
	}

	char read_schema_from_next_field() {
		char c = fgetc(_f);
		StrBuff field;
		
		// read in the field
		// read until we enter a field or the line is over.
		while(c != '\n' && c != EOF && c != '<') { c = fgetc(_f); }
		if(c == '\n' || c == EOF) return c; // reach end of line

		c = fgetc(_f); // read in first char of field

		// read in the rest of the field
		while(c != '\n' && c != EOF && c != '>') {
			field.addc(c);
			c = fgetc(_f);
		}
		String* field_str = field.get();
		char s_char = classify_field(field_str);
		delete(field_str);
		return s_char;
	}

	String* read_schema_from_line() {
		StrBuff schema_buf;
		char c = read_schema_from_next_field(); 
		while(c != '\n' && c != EOF) { // read to end of line or end of file
			if(c == 'M') return nullptr; // if a field is missing, this can't be the schema.
			schema_buf.addc(c); // add the schema character
			c = read_schema_from_next_field();
		}
		return schema_buf.get();
	}

	Schema* build() {
		String* best_fit = new String("");
		String* temp;
		int row = 0;
		// while we haven't reached the end of the file or past 500 lines
		while(!feof(_f) && row < 500) {
			temp = read_schema_from_line(); // get the schema from the line
			if(temp != nullptr) { // make sure we got a valid schema
				if(temp->size() > best_fit->size()) { // is it bigger?
					delete(best_fit);
					best_fit = temp;
				}
				else delete(temp);
			}
			row++; // increment rows
		}
		if(best_fit->size() == 0) assert(false); // check we found a schema.
		Schema* sch = new Schema(best_fit->c_str());
		delete(best_fit);
		return sch;
	}

	static Schema* build(char* path) {
		SOR_SchemaBuilder builder(path);
		return builder.build();
	}
};

/**
 * @brief      Builder to create frame objects step by step.
 */
class FrameBuilder {
public:
	FILE* _f;
	DataFrame* _df;

	FrameBuilder() {}

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
	 * @brief      Builds the dataframe. Consumes _df
	 *
	 * @param[in]  num_rows  The number of rows to read into the dataframe
	 *
	 * @return     the dataframe created from the given rows in the file
	 */
	DataFrame* build(size_t num_rows) {
		assert(_df != nullptr); // crash if we've already built the dataframe
		if(num_rows == 0) { // if no number of rows is provided, we just go until the end.
			while(read_row() != -1) continue; // continue until we reach EOF.
		}
		else {
			for (int i = 0; i < num_rows; ++i)
			{
				if(read_row() == -1) return _df; // end if we've reached end of file early
			}
		}
		DataFrame* df = _df;
		_df = nullptr;
		return df;
	}
};

class SOR_FrameBuilder : public FrameBuilder {
public:
	SOR_FrameBuilder(char* path, Schema& s) : FrameBuilder(path, s) { }
	SOR_FrameBuilder(const char* path, Schema& s) : FrameBuilder(path, s) { }

	SOR_FrameBuilder(char* path) : FrameBuilder() {
		// build the schema and then build the frame
		Schema* sch = SOR_SchemaBuilder::build(path);
		_f = fopen(path, "r");
		assert(_f != nullptr);

		_df = new DataFrame(*sch);
		delete(sch);
	}

	~SOR_FrameBuilder() { }

	String* clean_string(String* field) {
		char* cleaned = new char[field->size() + 1];
		cleaned[field->size()] = '\0';
		size_t idx = 0;

		for (int i = 0; i < field->size(); ++i)
		{
			if(field->at(i) == ' ' && idx == 0) { continue; } // space spadding
			if(field->at(i) == '\"' && idx != 0) { break; } // found end quote. Chop off remaining input.
			if(field->at(i) == '\"') { continue; } // found first quote
			cleaned[idx++] = field->at(i);
		}
		cleaned[idx] = '\0';
		delete(field);
		String* cleaned_str = new String(cleaned);
		delete[](cleaned);
		return cleaned_str;
	}

	String* clean_primitive(String* field, char type) {
		char* cleaned = new char[field->size() + 1];
		size_t idx = 0;

		for (int i = 0; i < field->size(); ++i)
		{
			if(field->at(i) == ' ' && idx != 0) break;
			if(field->at(i) == ' ') { continue; } // space spadding
			cleaned[idx++] = field->at(i);
		}
		cleaned[idx] = '\0';
		delete(field);

		// check validity, if invalid return base value
		char* validity_check = nullptr;
		switch(type) {
			case 'I':
				validity_check = to_str<int>(atoi(cleaned));
				if(strcmp(validity_check, cleaned) != 0) { delete[](cleaned); delete[](validity_check); return new String("0"); }
				break;
			case 'F':
				validity_check = to_str<float>((float)atof(cleaned));
				if(strcmp(validity_check, cleaned) != 0) { delete[](cleaned); delete[](validity_check); return new String("0"); }
				break;
			case 'B':
				validity_check = to_str<int>(atoi(cleaned));
				if(strcmp(validity_check, cleaned) != 0) { delete[](cleaned); delete[](validity_check); return new String("0"); }
				break;
			default:
				return new String("0");
		}
		if(validity_check != nullptr) delete[](validity_check);

		String* cleaned_str = new String(cleaned);
		delete[](cleaned);
		return cleaned_str;

	}

	String* clean_field(String* field, char type) {
		switch(type) {
			case 'S':
				return clean_string(field);
			default:
				return clean_primitive(field, type);
		}
	}

	int read_field(Row& r, int col) {
		int inQuotes = 0; // 0 - no quote found, 1 - first quote found, 2 - second quote found
		// read to next '<'
		char c = fgetc(_f);
		while(c != '<') {
			if(c == EOF || c == '\n') return -1;
			c = fgetc(_f);
		}

		// next char is first char of field
		c = fgetc(_f);
		StrBuff field;
		while(c != '>' || inQuotes == 1) {
			if(c == EOF || c == '\n') return -1;
			if(c == '\"') { inQuotes++; }
			field.addc(c);
			c = fgetc(_f);
		}
		String* cleaned = clean_field(field.get(), r.col_type(col));
		add_field_to_row(cleaned, r, col);
		delete(cleaned);
		return col;
	}

	int read_row() {
		//char c = fgetc(_f);
		if(feof(_f)) return READ_ROW_EOF_FAIL; // fail (found end of file)

		Row* r = new Row(_df->get_schema());
		int col = 0;
		while(read_field(*r, col) != -1) { col++; }
		while(col < _df->ncols()) {
			add_field_to_row(clean_field(new String(""), r->col_type(col)), *r, col); // set the rest to default values
			col++;
		}
		_df->add_row(*r);
		delete(r);
		return READ_ROW_SUCCESS;
	}
};