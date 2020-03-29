#include <assert.h>
#include "dataframe.h" 

// Test all classes' constructors and methods

void testSchema() {
	// constructors
	Schema def;
	assert(def.width() == 0 && def.length() == 0);
	def.add_column('I', nullptr);
	def.add_column('B', nullptr);
	def.add_column('S', nullptr);
	Schema s("IBS");
	assert(def.equals(&s));
	assert(def.width() == 3);

	// copy and add column
	String* colName = new String("float");
	def.add_column('F', colName);
	Schema copy(def);
	assert(def.equals(&copy));
	assert(def.col_name(3)->equals(colName));
	assert(def.ncol == 4);

	// add row
	def.add_row(nullptr);
	assert(def.length() == 1);
	assert(def.row_name(0) == nullptr);
	def.add_row(colName);
	assert(def.length() == 2);
	assert(def.row_name(1)->equals(colName));

	delete(colName);
	exit(0);
}

void testRow() {
	// constructors
	Schema s("IB");
	Row r(s);
	Row r2(s);
	assert(r.width() == 2);
	assert(r.col_type(0) == 'I');
	assert(r.col_type(1) == 'B');

	// other methods
	assert(r.data_equals(&r2));
	r.set_idx(5);
	assert(r.get_idx() == 5);
	assert(!r.data_equals(&r2));
	exit(0);
}

void testColumn() {
	// constructors
	Column* c1 = new IntColumn(4, 1, 2, 3, 4);
	Column* c2 = new FloatColumn(2, 1.5, 10.7);
	Column* c3 = new BoolColumn(1, 1);
	Column* c4 = new StringColumn();

	// size
	assert(c1->size() == 4);
	assert(c2->size() == 2);
	assert(c3->size() == 1);
	assert(c4->size() == 0);

	// get_type
	assert(c1->get_type() == 'I');
	assert(c2->get_type() == 'F');
	assert(c3->get_type() == 'B');
	assert(c4->get_type() == 'S');

	// push_back
	c1->push_back(10);
	c2->push_back(100.9f);
	c3->push_back(false);
	String* s = new String("test");
	c4->push_back(s);

	assert(c1->size() == 5);
	assert(c2->size() == 3);
	assert(c3->size() == 2);
	assert(c4->size() == 1);

	// as_types
	IntColumn* ic = c1->as_int();
	IntColumn* bad = c2->as_int();
	FloatColumn* fc = c2->as_float();
	BoolColumn* bc = c3->as_bool();
	StringColumn* sc = c4->as_string();
	assert(ic != nullptr);
	assert(bad == nullptr);
	assert(fc != nullptr);
	assert(bc != nullptr);
	assert(sc != nullptr);

	// get / set
	assert(ic->get(4) == 10);
	assert(fc->get(2) == 100.9f);
	assert(bc->get(1) == false);
	assert(sc->get(0)->equals(s));

	String* newS = new String("not test");
	ic->set(4, -10);
	fc->set(2, -100.9);
	bc->set(1, true);
	sc->set(0, newS);

	assert(ic->get(4) == -10);
	assert(fc->get(2) == -100.9f);
	assert(bc->get(1) == true);
	assert(!sc->get(0)->equals(s));
	assert(sc->get(0)->equals(newS));

	// clone / equals
	Column* copy = new IntColumn(5, 1, 2, 3, 4, -10);
	assert(copy->data_equals(ic));
	Object* clone = sc->clone();
	assert(sc->data_equals(clone));

	// cleanup
	delete(ic);
	delete(fc);
	delete(bc);
	delete(sc);
	delete(copy);
	delete(clone);
	delete(s);
	delete(newS);
	exit(0);
}

void testDataFrame() {
	String* name = new String("Hello");

	// constructor
	Schema s("IFBS");
	DataFrame ds(s);
	assert(s.equals(&ds.get_schema()));
	assert(ds.nrows() == 0);
	assert(ds.ncols() == 4);

	// get name not in frame
	String no("I'm not in the dataframe");
	assert(ds.get_col(no) == -1);
	assert(ds.get_row(no) == -1);

	// add_row and add_column
	Row r(ds.get_schema());
	for (int i = 0; i < 5; ++i)
	{
		r.set(0, i * 10 + 1);
		r.set(1, i * -0.1f);
		r.set(2, i % 2 == 0);
		r.set(3, name);
		ds.add_row(r);
	}

	IntColumn* col = new IntColumn(5, -1, -11, -21, -31, -41);
	ds.add_column(col, name);
	assert(!s.equals(&ds.get_schema()));
	assert(ds.ncols() == 5);
	assert(ds.get_col(*name) == 4);

	for (int i = 0; i < 5; ++i)
	{
		assert(ds.get_int(0, i) == i * 10 + 1);
		assert(ds.get_float(1, i) == i * -0.1f);
		assert(ds.get_bool(2, i) == (i % 2 == 0));
		assert(ds.get_string(3, i)->equals(name));
		assert(ds.get_int(4, i) == i * -10 - 1);
	}

	// fill_row
	Row r2(ds.get_schema());
	ds.fill_row(4, r2);
	assert(!r.equals(&r2));
	assert(r2.get_int(0) == 41);
	assert(r2.get_float(1) == -0.4f);
	assert(r2.get_bool(2) == true);
	assert(r2.get_string(3)->equals(name));
	assert(r2.get_int(4) == -41);

	// equals
	DataFrame ds2(s);
	DataFrame ds3(s);
	assert(!ds.data_equals(&ds2));
	assert(ds2.data_equals(&ds3));

	// clone
	DataFrame dsClone(ds);
	assert(dsClone.ncols() == ds.ncols());
	assert(dsClone.nrows() != ds.nrows());
	assert(dsClone.get_col(*name) == ds.get_col(*name));

	// cleanup
	delete(name);
	exit(0);
}

// This test is checking the functionality of the Schema class. 
// It first checks equivalence between creating a new Schema piece by piece using the add_column function and through the constructor.
// It then checks the functionality of our copy constructor for Schema, by ensuring it successfully copied over a column name in the correct position.
// The last check is showing that adding a nullptr row name is possible, and will grow the length of the dataframe.

void testSchemaFunctionality() {
	// constructors
	Schema def;
	assert(def.width() == 0 && def.length() == 0);
	def.add_column('I', nullptr);
	def.add_column('B', nullptr);
	def.add_column('S', nullptr);
	Schema s("IBS");
	assert(def.equals(&s)); // check if we are equal to a similar schema
	assert(def.width() == 3); // check the width

	// copy and add column
	String* colName = new String("float");
	def.add_column('F', colName); // add a new column
	Schema copy(def); // copy the schema
	assert(def.equals(&copy)); // check equality
	assert(def.col_name(3)->equals(colName)); // check name equality
	assert(def.width() == 4); // check width

	// add row
	def.add_row(nullptr); // alright let's add a row
	assert(def.length() == 1); // check length
	assert(def.row_name(0) == nullptr); // check row name
	def.add_row(colName); // add a row with a name
	assert(def.length() == 2); // check length
	assert(def.row_name(1)->equals(colName)); // check row name

	// clean up
	delete(colName);
	// exit(0);
}

// This test is checking semantics on the Row class implementation.
// It first checks that the Row constructor successfully translates the given Schema into a valid Row object.
// Next, it checks equality between two equivalent Rows, the checks field altering and data setting.

void testRowSemantics() {
	// constructors: create a schema and create two rows with the same schema
	Schema s("IB");
	Row r(s);
	Row r2(s);
	// check width and column types for first row
	assert(r.width() == 2);
	assert(r.col_type(0) == 'I');
	assert(r.col_type(1) == 'B');

	// check similarity
	assert(r.equals(&r2) == false); // not same pointer
	for (int i = 0; i < r.width(); ++i)
	{
		assert(r.col_type(i) == r2.col_type(i)); // check columns correctly correspond
	}

	// set index
	r.set_idx(5); // set index
	assert(r.get_idx() == 5);

	// set
	r2.set(0, 5);
	r2.set(1, (bool)false);

	assert(r2.get_int(0) == 5);
	assert(r2.get_bool(1) == false);

	// exit(0);
}

// This test is checking the functionality of the Column class implementation.
// We first create 4 Column representations, one for each of the supported types.
// Our first check is that our size function correctly returns the size of the newly created Columns.
// Next, we check that get_type is correctly able to identify the type of all supported Column types.
// Following this, we check that push_back is able to add to the Column and has the side effect of growing the Column's size.
// After, we check that our as_<type> functions are able to correctly convert to the selected type Column (by checking if the cast is nullptr)
// A simple test for our get/set follows, ensuring that values are in their correct places within our Columns and thus, dataframes.
// Finally, we ensure that our equality and cloning functionality is proper by comparing newly created clones of existing arrays and their data.

void testColumnFunctionality() {
	// constructors
	Column* c1 = new IntColumn(4, 1, 2, 3, 4);
	Column* c2 = new FloatColumn(2, 1.5, 10.7);
	Column* c3 = new BoolColumn(1, 1);
	Column* c4 = new StringColumn();

	// size
	assert(c1->size() == 4);
	assert(c2->size() == 2);
	assert(c3->size() == 1);
	assert(c4->size() == 0);

	// get_type
	assert(c1->get_type() == 'I');
	assert(c2->get_type() == 'F');
	assert(c3->get_type() == 'B');
	assert(c4->get_type() == 'S');

	// push_back
	c1->push_back(10);
	c2->push_back(100.9f);
	c3->push_back(false);
	String* s = new String("test");
	c4->push_back(s);

	// check updated size
	assert(c1->size() == 5); 
	assert(c2->size() == 3);
	assert(c3->size() == 2);
	assert(c4->size() == 1);

	// as_types
	IntColumn* ic = c1->as_int();
	IntColumn* bad = c2->as_int();
	FloatColumn* fc = c2->as_float();
	BoolColumn* bc = c3->as_bool();
	StringColumn* sc = c4->as_string();
	// check the as_type methods don't return nullptrs (or do if used invalidly)
	assert(ic != nullptr);
	assert(bad == nullptr);
	assert(fc != nullptr);
	assert(bc != nullptr);
	assert(sc != nullptr);

	// get / set
	// check original data
	assert(ic->get(4) == 10);
	assert(fc->get(2) == 100.9f);
	assert(bc->get(1) == false);
	assert(sc->get(0)->equals(s));

	// set new data
	String* newS = new String("not test");
	ic->set(4, -10);
	fc->set(2, -100.9);
	bc->set(1, true);
	sc->set(0, newS);

	// check the set data is correct
	assert(ic->get(4) == -10);
	assert(fc->get(2) == -100.9f);
	assert(bc->get(1) == true);
	assert(!sc->get(0)->equals(s));
	assert(sc->get(0)->equals(newS));

	// clone / equals
	Column* copy = new IntColumn(5, 1, 2, 3, 4, -10);
	assert(copy->size() == ic->size()); // same size!
	assert(copy->as_int()->get(4) == ic->as_int()->get(4)); // same data!
	assert(copy->equals(ic) == false); // not same pointer
	Column* clone = dynamic_cast<Column *>(sc->clone());
	assert(clone->size() == sc->size()); // same size!
	assert(clone->as_string()->get(0)->equals(sc->as_string()->get(0))); // same data!
	assert(!sc->equals(clone)); // not same pointer

	// cleanup
	delete(ic);
	delete(fc);
	delete(bc);
	delete(sc);
	delete(copy);
	delete(clone);
	delete(s);
	delete(newS);
	// exit(0);
}

// This test is checking the functionality of the Dataframe class implementation.
// We begin by creating a new Dataframe and Schema object. We ensure that the created Schema object is equal to the one located in Dataframe object.
// Then, we check the case where a user may attempt to get a column/row by name that doesn't currently exist in the Dataframe.
// Following this, we ensure correctness of our add_row and add_column functions by adding a somewhat complex example, 
//      and verifying that each value is at the correct index in a loop.
// After, we check that fill_row is properly implemented by calling the function and checking each individual value in the row.
// Lastly, we have two relatively simple checks for valid equality and cloning.

void testDataframeFunctionality() {
	String* name = new String("Hello"); // set up string to be used

	// constructor
	Schema s("IFBS");
	DataFrame ds(s);
	assert(s.equals(&ds.get_schema())); // check the schema is correct
	assert(ds.nrows() == 0); // we shouldn't have any rows yet
	assert(ds.ncols() == 4); // but we should have columns

	// get name not in frame
	String no("I'm not in the dataframe");
	assert(ds.get_col(no) == -1); // we should fail to find no in columns
	assert(ds.get_row(no) == -1); // same in rows

	// add_row and add_column
	// add 5 rows with dynamic(ish) data
	Row r(ds.get_schema());
	for (int i = 0; i < 5; ++i)
	{
		r.set(0, i * 10 + 1);
		r.set(1, i * -0.1f);
		r.set(2, i % 2 == 0);
		r.set(3, name);
		ds.add_row(r);
	}

	// add one column
	IntColumn* col = new IntColumn(5, -1, -11, -21, -31, -41);
	ds.add_column(col, name);
	assert(!s.equals(&ds.get_schema())); // make sure schema changed
	assert(ds.ncols() == 5); // columns should have incremented
	assert(ds.get_col(*name) == 4); // found column should be the last index (4)

	// check the stored data is correct.
	for (int i = 0; i < 5; ++i)
	{
		assert(ds.get_int(0, i) == i * 10 + 1);
		assert(ds.get_float(1, i) == i * -0.1f);
		assert(ds.get_bool(2, i) == (i % 2 == 0));
		assert(ds.get_string(3, i)->equals(name));
		assert(ds.get_int(4, i) == i * -10 - 1);
	}

	// fill_row
	Row r2(ds.get_schema());
	ds.fill_row(4, r2); // fill the row with the data from the last row
	assert(r.equals(&r2) == false); // make sure row pointers are different
	// check stored data
	assert(r2.get_int(0) == 41);
	assert(r2.get_float(1) == -0.4f);
	assert(r2.get_bool(2) == true);
	assert(r2.get_string(3)->equals(name));
	assert(r2.get_int(4) == -41);

	// equals
	DataFrame ds2(s);
	DataFrame ds3(s);
	assert(!ds.equals(&ds2)); // the schema for ds changed, so we should not be equal
	assert(ds2.get_schema().equals(&ds3.get_schema())); // the schemas are identical

	// clone
	DataFrame dsClone(ds);
	assert(dsClone.ncols() == ds.ncols()); // column number should be the same
	assert(dsClone.nrows() != ds.nrows()); // rows should not
	assert(dsClone.get_col(*name) == ds.get_col(*name)); // column names should be the same

	// cleanup
	delete(name);
	exit(0);
}

// In this test, we are creating a custom Fielder implementation that collects data of users and their stored IDs (int).
// We will then map the Fielder onto the Dataframe. We will implement a Rower that interates through the Dataframe to collect the corresponding data.

int main(int argc, char** argv) {
    testSchema();
    testRow();
    testColumn();
    testDataFrame();

    testSchemaFunctionality();
    testRowSemantics();
    testColumnFunctionality();
    testDataframeFunctionality();

    return 0;
}