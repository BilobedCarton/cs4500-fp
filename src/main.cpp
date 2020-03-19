#include <assert.h>

#include "dataframe/dataframe.h"
#include "dataframe/frame_builder.h"


enum class OperationType { GET, TYPE, PRINT, INVALID };

OperationType GetOperationFromInput(char* input) {
	if(strcmp(input, "GET") == 0) return OperationType::GET;
	else if(strcmp(input, "TYPE") == 0) return OperationType::TYPE;
	else if(strcmp(input, "PRINT") == 0) return OperationType::PRINT;
	return OperationType::INVALID;
}

class Parameters : public Sys {
public:
	char* path;
	OperationType op;
	size_t col;
	size_t row;

	Parameters() {
		path = nullptr;
		op = OperationType::INVALID;
		col = -1;
		row = -1;
	}

	~Parameters() {}

	void HandleFlag(char* flag, char* input) {
		if(input[0] == '-') {
			p("Invalid input: ");
			pln(input);
		}

		if(strcmp(flag, "-f") == 0) path = input;
		else if(strcmp(flag, "-col") == 0) col = atoi(input);
		else if(strcmp(flag, "-row") == 0) row = atoi(input);
		else if(strcmp(flag, "-op") == 0) op = GetOperationFromInput(input);
		else {
			p("Invalid flag: ");
			pln(flag);
		}
	}

	void Verify() {
		bool bad = false;
		if(path == nullptr) { pln("Invalid or no path specified."); bad = true; }
		if(op == OperationType::INVALID) { pln("Invalid or no operation specified."); bad = true; }
		if(col == -1 && op != OperationType::PRINT) { pln("Invalid or no column specified."); bad = true; }
		if(row == -1 && (op != OperationType::TYPE && op != OperationType::PRINT)) { pln("Invalid or no row specified."); bad = true; }
		if(bad) { assert(false); }
	}
};

void executeOperation(DataFrame& df, OperationType ot, size_t col, size_t row) {
	switch(ot) {
		case OperationType::GET:
			df.p("Value at column ").p(col).p(" and row ").p(row).p(" is: ");
			switch(df.get_schema().col_type(col)) {
				case 'I':
					df.pln(df.get_int(col, row));
					return;
				case 'F':
					df.pln(df.get_float(col, row));
					return;
				case 'B':
					df.pln(df.get_bool(col, row));
					return;
				case 'S':
					df.pln(df.get_string(col, row)->c_str());
					return;
				default:
					assert(false);
					return;
			}
		case OperationType::TYPE:
			df.p("Type of column ").p(col).p(" is: ").pln(df.get_schema().col_type(col));
			return;
		case OperationType::PRINT:
			df.print();
			return;
		case OperationType::INVALID:
			df.p("Attempting invalid operation.");
			assert(false);
			return;
	}
}

DataFrame* buildFrame(char* path) {
	SOR_FrameBuilder builder(path);
	DataFrame* frame = builder.build(0);
	assert(frame != 0);
	return frame;
}

int main(int argc, char** argv) {
	assert(argc % 2 == 1); // should have a even number of flag/value inputs and 1 program name

	Parameters pars;
	for (int i = 1; i < argc; i+=2)
	{
		pars.HandleFlag(argv[i], argv[i+1]);
	}
	pars.Verify();

	DataFrame* frame = buildFrame(pars.path);

	executeOperation(*frame, pars.op, pars.col, pars.row);

	delete(frame);
}