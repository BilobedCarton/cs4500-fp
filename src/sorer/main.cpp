// lang::CwC

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "utils/helper.h"
#include "field.h"
#include "utils/object.h"
#include "utils/string.h"
#include "sorfile.h"


/**
 * @brief      Checks the given flag and returns the corresponding flag code.
 *
 * @param      flag  The flag
 *
 * @return     a flag code corresponding to the given flag.
 */
int check_flag(char* flag) {
    if(strcmp(flag, "-f") == 0) {
        return 1;
    } else if (strcmp(flag, "-from") == 0) {
        return 2;
    } else if (strcmp(flag, "-len") == 0) {
        return 3;
    } else if (strcmp(flag, "-print_col_type") == 0) {
        return 4;
    } else if (strcmp(flag, "-print_col_idx") == 0) {
        return 5;
    } else if (strcmp(flag, "-is_missing_idx") == 0) {
        return 6;
    } else {
        return -1;
    }
}


// @brief      Compare the given int and string values for equivalence
//
// @param[in]  i     an int
// @param      str   a string
//
// @return     true if the string correctly represents the int
//
bool correctNumberInt(int i, char* str) {
    char buf[33];
    sprintf(buf, "%d", i);
    return strcmp(buf, str) == 0;
}

/**
 * @brief      Checks if the string is a correct representation of the given float
 *
 * @param[in]  f     a float
 * @param      str   The string representing the float
 *
 * @return     True if the string correctly represents the float
 */
bool correctNumberFloat(float f, char* str) {
    return (strcmp(str, float_to_string(f)) == 0);
}

long getFileSize(const char* file_path) {
    FILE* file = fopen(file_path, "r");
    assert(file != NULL);

    fseek(file, 0 , SEEK_END);
    long fsize = ftell(file);
    fclose(file);

    return fsize;
}

/**
 * @brief      Opens a file and returns the contents as a char array.
 *
 * @param[in]  file_path  The file path
 *
 * @return     the contents of the file as a char array
 */
char* open_file(const char* file_path, size_t start, size_t end) {
    // open and store file contents in a char array
    FILE* file = fopen(file_path, "r");
    assert(file != NULL);

    long fsize = getFileSize(file_path);

    assert(start < fsize); // ensure start in bounds
    end = (fsize - start < end) ? fsize - start : end; // This is to bound end to the size of the file 
    // such that we can give a larger end and just read the rest of the file

    char* file_string = new char[end + 1];
    fseek(file, start, SEEK_SET);
    fread(file_string, 1, end, file);
    fclose(file);
    file_string[end] = '\0';

    return file_string;
}


/**
 * @brief      strip excess space from the given field
 *
 * @param      field  The field
 *
 * @return     the field with excess white space removed, nullptr if the field is invalid.
 */
char* preprocessField(char* field) {
    int startIdx = 0;
    size_t endIdx;
    bool inQuotes = false;
    // strip excess space and delete invalid fields?
    for (int i = 0; i < strlen(field); ++i)
    {
        if(field[i] != ' ' && startIdx == -1) {
            startIdx = i;
            if(field[i] == '\"') inQuotes = true;
        }
        if(field[i] == '\"' && inQuotes) {
            endIdx = i + 1;
            // check rest are white spaces.
            for (int j = i + 1; j < strlen(field); ++j)
            {
                if(field[j] != ' ') return nullptr; // invalid
            }
            break;
        }
        if(field[i] == ' ' && startIdx != -1) {
            endIdx = i;
            // check rest are white spaces
            for (int j = i + 1; j < strlen(field); ++j)
            {
                if(field[j] != ' ') return nullptr; // invalid
            }
            break;
        }
    }
    endIdx = strlen(field);
    char* field_proc = new char[endIdx - startIdx + 1];
    strncpy(field_proc, &field[startIdx], endIdx - startIdx);
    field_proc[endIdx - startIdx] = '\0';
    return field_proc;
}

// @brief      Parse the clean char* for the field type
//
// @param      str   The char*
//
// @return     The field type.
//
size_t getFieldType(char* str) {

    // check if a string - contains quote
    if(str[0] == '\"') {
        if(str[strlen(str) - 1] == '\"') {
            return STRING_TYPE;
        }
        for(int i = 0; i < strlen(str); i++) {
            if(str[i] == ' ') {
                return INVALID; 
            }
        }
        return STRING_TYPE;
    }
    // check strings w/o "" 1234567890.-
    for(int i = 0; i < strlen(str); i++) {
        if(i == 0) {
            if(!(isdigit(str[i]) || str[i] == '.' || str[i] == '-' || str[i] == '+')) { 
                return STRING_TYPE; 
            }
        }
        if(!(isdigit(str[i]) || str[i] == '.')) {
            return STRING_TYPE; 
        }
    }
    // is it a float?
    float f = strtof(str, NULL);
    if(correctNumberFloat(f, str)) {
        return FLOAT_TYPE; 
    }
    // is it a bool?
    if(strcmp(str, "0") == 0 || strcmp(str, "1") == 0) {
        return BOOL_TYPE; 
    }
    // it's an int.
    int i = atoi(str);
    if(correctNumberInt(i, str)) { 
        return INT_TYPE; 
    }

    return INVALID;
}

/**
 * @brief      Gets the schema from the given line.
 *
 * @param      line       The line
 * @param[in]  numFields  the number of fields held in the line
 *
 * @return     The schema dictated in the line terminated with an INVALID
 */
size_t* getSchemaFromLine(char* line, size_t numFields) {
    // make sure we have a spot for the terminator (an INVALID)
    size_t* schema = new size_t[numFields + 1];
    size_t schemaIdx = 0;
    bool inField = false;
    bool inQuote = false;
    bool badRow = false;
    GrowableString* field = new GrowableString("");
    // loop through the line
    for (int i = 0; i < strlen(line); i++) {
        // are we ready to end a field?
        if ((line[i] == '>' && inField == true && !inQuote) || badRow) {
            inField = false;
            badRow = false;
            char* field_proc = preprocessField(field->str_);
            schema[schemaIdx++] = getFieldType(field_proc);
            delete[](field_proc);
            delete field;
            // double check we got a valid field so that this can actually serve as a schema.
            if (schema[schemaIdx - 1] == INVALID || schema[schemaIdx - 1] == MISSING_TYPE) {
                delete[] schema;
                return nullptr;
            }
            field = new GrowableString("");
        }
        
        // are we in a field?
        if (inField == true) {
            if(line[i] == ' ' && !inQuote) {
                // flag this as a bad row if we find a space in a non-quote delimited string.
                badRow = true;
                continue;
            }
            // handle quotes
            if(line[i] == '\"' && !inQuote) inQuote = true;
            else if(line[i] == '\"') inQuote = false;
            field->add_char(line[i]);
        }

        // we're ready to start a field. No need to care about quotes since we're not in a field.
        if (line[i] == '<' && inField == false) {
            inField = true;
        }
    }
    delete field;
    // check we properly ended the line...
    if (inField) {
        delete[] schema;
        return nullptr;
    }
    assert(numFields > 0);
    schema[numFields] = INVALID;
    return schema;
}

/**
 * @brief      Gets the schema from the given file by reading at most the first 500 lines
 *
 * @param      fName  The file name
 *
 * @return     The schema of the given file.
 */
size_t* getSchema(char* fName) {
    FILE* file = fopen(fName, "r");
    assert(file != NULL);
    char* longestLine = new char[0];
    size_t longestFields = 0;
    bool inField = false;
    bool inQuote = false;

    char c = (char) fgetc(file);
    for (int i = 0; i < 500; i++) {
        // are we at the end?
        if (c == EOF) {
            break;
        }
        // okay then we're at the start of the line.
        size_t curFields = 0;
        inField = false;
        GrowableString* curLine = new GrowableString("");

        // we go to EOF or next newline (unless in quote)
        while ((c != '\n' || (c == '\n' && inQuote)) && c != EOF) {
            // start the field
            if (c == '<' && inField == false) {
                inField = true;
            }
            // end unless we are in quotes
            if (c == '>' && inField == true && !inQuote) {
                curFields++;
                inField = false;
            }
            // if we start (or end) a quote we need to flip that flag
            if (c == '\"' && !inQuote) inQuote = true;
            else if (c == '\"') inQuote = false;
            
            curLine->add_char(c);
            c = (char) fgetc(file);
        }
        // if we're at EOF we need to discard this line, hopefully we found an end so data is saved, 
        // but if not, then the line was poorly formed.
        if (c == EOF) {
            delete curLine;
            break;
        }
        // did we find a better option for schema?
        if (curFields > longestFields) {
            // was it valid?
            if (getSchemaFromLine(curLine->str_, curFields) == nullptr) {
                delete curLine;
                c = (char) fgetc(file);
                continue;
            }
            // okay lets record it.
            longestFields = curFields;
            delete[] longestLine;
            longestLine = new char[curLine->size_ + 1];
            strcpy(longestLine, curLine->str_);
        } 
        c = (char) fgetc(file);
        delete curLine;
    }
    // okay now let's build the schema.
    size_t* schema = getSchemaFromLine(longestLine, longestFields);
    delete[](longestLine);
    fclose(file);
    assert(schema != nullptr);
    return schema;
}

/**
 * @brief      Gets the number of lines in the given string lines.
 *
 * @param      str   The string
 *
 * @return     The number of lines.
 */
size_t getNumLines(char* str) {
    size_t count = 0;
    for (int i = 0; i < strlen(str); i++) {
        if (str[i] == '\n') {
            count++;
        }
    }
    return count;
}

/**
 * @brief      Reads len bytes from a given SoR file from start and creates a SorFile representing those bytes.
 *
 * @param      fName  The file name
 * @param[in]  start  The start position
 * @param[in]  len    The length of bytes to be read
 *
 * @return     { description_of_the_return_value }
 */
SorFile* readFile(char* fName, size_t start, size_t len) {
    // get schema from first 500 lines before reading from start.
    size_t* schema = getSchema(fName);
    size_t schemaSize = 0;

    for (int i = 0; schema[i] != INVALID; i++) {
        schemaSize++;
    }

    SorFile* sorFile = new SorFile(schema, schemaSize);

    // read from start.
    char* file = open_file(fName, start, len);

    bool discardStart = false;
    bool discardEnd = false;
    if (start != 0) {
        discardStart = true;
    }
    if (len <= getFileSize(fName) - start) {
        discardEnd = true;
    }

    size_t numLines = getNumLines(file);
    size_t curLine = 1;
    size_t curCol = 0;
    bool inField = false;
    bool inQuote = false;
    GrowableString* field = new GrowableString("");
    for (int i = 0; i < len; i++) {
        // found a non in-field new line?
        if ((file[i] == '\n' && !inField) || curCol > schemaSize) {
            if (discardStart) {
                discardStart = false;
                continue;
            }
            if(curCol > schemaSize) sorFile->discard_row(curLine - 1);
            while(curCol < schemaSize) sorFile->add_field(curCol++, new Field(MISSING_TYPE, NULL));
            curLine++;
            curCol = 0;
            inField = false;
            inQuote = false;
            continue;
        }
        if (discardStart) continue;
        if (discardEnd && curLine == numLines + 1) break;
        // did we find the end of a field?
        if (file[i] == '>' && inField && !inQuote) {
            Field* f;
            char* field_proc = preprocessField(field->str_);
            if(field_proc == NULL) f = new Field(MISSING_TYPE, NULL);
            else f = new Field(schema[curCol], field_proc);
            delete[] (field_proc);
            sorFile->add_field(curCol, f);
            delete field;
            field = new GrowableString("");
            inField = false;
            curCol++;
        }
        if (inField) {
            if(file[i] == ' ' && !inQuote) {
                curCol = schemaSize + 1;
                continue;
            }
            if(file[i] == '\"' && !inQuote) inQuote = true;
            else if(file[i] == '\"') inQuote = false;
            field->add_char(file[i]);
        }
        if (file[i] == '<' && !inField) {
            inField = true;
        }
    }
    delete(field);
    delete[](file);
    delete[](schema);
    return sorFile;
}

// @brief      get the column type at the given column index
//
// @param[in]  colIdx  The column index
// @param      file    The file
//
// @return     The column type.
//
size_t getColType(size_t colIdx, SorFile* file) {
    return file->get_col_type(colIdx);
}

// @brief      return the field at the given column/row
//
// @param[in]  colIdx  The column index
// @param[in]  rowIdx  The row index
// @param      file    The file
//
// @return     The field at the given position.
//
Field* getField(size_t colIdx, size_t rowIdx, SorFile* file) {
    return file->get_field(colIdx, rowIdx); 
}

// @brief      Is there a missing field in the specified column offset?
//
// @param[in]  colIdx  The column index
// @param[in]  rowIdx  The row index
//
// @return     True if field is missing, False otherwise.
//
bool isFieldMissing(size_t colIdx, size_t rowIdx, SorFile* file) {
    Field* f = file->get_field(colIdx, rowIdx);
    if(f == nullptr) return true;
    return f->is_missing();
}

/**
 * @brief      Gets the type definition.
 *
 * @param[in]  type  The type
 *
 * @return     The type definition as a char*.
 */
const char* getTypeDefinition(size_t type) {
    assert(type >= 0 && type < 5);
    switch(type) {
        case 0:
            return "INTEGER";
            break;
        case 1:
            return "FLOAT";
            break;
        case 2:
            return "STRING";
            break;
        case 3:
            return "BOOL";
            break;
        case 4:
        default:
            assert(false);
            break;
    }
    return nullptr;
}

/**
 * @brief      main function
 *
 * @param[in]  argc  The count of arguments
 * @param      argv  The arguments array
 *
 * @return     program return code
 */
int main(int argc, char** argv) {

    assert(argc > 8);

    char* fileName;
    size_t startPos;
    size_t bytesToRead;
    size_t columnIdx;
    size_t rowIdx;
    size_t commandID = 0;

    for (int i = 1; i < argc; i++) {
        int flag = check_flag(argv[i]);
        switch(flag) {
            case 1: // file name
                fileName = argv[i+1];
                i++;
                break;
            case 2: // start postition in file (in bytes)
                startPos = atoi(argv[i+1]);
                assert(correctNumberInt(startPos, argv[i+1]));
                i++;
                break;
            case 3: // number of bytes to read
                bytesToRead = atoi(argv[i+1]);
                assert(correctNumberInt(bytesToRead, argv[i+1]));
                i++;
                break;
            case 4: // print the type of a column
                columnIdx = atoi(argv[i+1]);
                assert(correctNumberInt(columnIdx, argv[i+1]));
                commandID = 4;
                i++;
                break;
            case 5: // the first argument is the column, second is the offset
                assert(argc > 9);
                columnIdx = atoi(argv[i+1]);
                assert(correctNumberInt(columnIdx, argv[i+1]));
                rowIdx = atoi(argv[i+2]);
                assert(correctNumberInt(rowIdx, argv[i+2]));
                commandID = 5;
                i = i + 2;
                break;
            case 6: // is there a missing index in the specified column offset
                assert(argc > 9);
                columnIdx = atoi(argv[i+1]);
                assert(correctNumberInt(columnIdx, argv[i+1]));
                rowIdx = atoi(argv[i+2]);
                assert(correctNumberInt(rowIdx, argv[i+2]));
                commandID = 6;
                i = i + 2;
                break;
            case -1:
            default:
                exit(1); // argument list malformed
                break;
        }
    }
    SorFile* sorfile = readFile(fileName, startPos, bytesToRead);
    Field* f = nullptr;

    switch(commandID) {
        case 4: // print column type
            switch(getColType(columnIdx, sorfile)) {
                case INT_TYPE:
                    println("INTEGER");
                    break;
                case FLOAT_TYPE:
                    println("FLOAT");
                    break;
                case STRING_TYPE:
                    println("STRING");
                    break;
                case BOOL_TYPE:
                    println("BOOL");
                    break;
                default:
                    assert(false);
                    break;
            }
            break;
        case 5: // print data
            f = getField(columnIdx, rowIdx, sorfile);
            assert(f != nullptr);
            if(f->type_ == STRING_TYPE && f->value_[0] != '\"') {
                print("\"");
                print(f->value_);
                println("\"");
            }
            else f->value_ == NULL ? println("MISSING") : println(f->value_);
            break;
        case 6: // is missing?
            if(isFieldMissing(columnIdx, rowIdx, sorfile)) println(1);
            else println(0);
            break;
        default: 
            assert(false);
            break;
    }

    delete(sorfile);
    return 0;

}