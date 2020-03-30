run: build test clean

build: main

test:
	-./main -f ../tests/allTypes.sor -op PRINT
	-./main -f ../tests/data.sor -op TYPE -col 2
	-./main -f ../tests/allTypes.sor -op GET -col 3 -row 2

main:
	g++ -o main -g -Wall -std=c++17 ./src/main.cpp

valgrind:
	valgrind --leak-check=yes --track-origins=yes ./main.cpp

unit-tests: build-tests run-tests clean-tests

build-tests:
	cd ./tests; g++ -o testKey -Wall -std=c++17 ./store/testKey.cpp
	cd ./tests; g++ -o testKVStore -Wall -std=c++17 ./store/testKVStore.cpp
	cd ./tests; g++ -o testValue -Wall -std=c++17 ./store/testValue.cpp
	cd ./tests; g++ -o testSchema -Wall -std=c++17 ./dataframe/testSchema.cpp
	cd ./tests; g++ -o testRow -Wall -std=c++17 ./dataframe/testRow.cpp
	cd ./tests; g++ -o testColumn -Wall -std=c++17 ./dataframe/testColumn.cpp
	cd ./tests; g++ -o testDataframe -Wall -std=c++17 ./dataframe/testDataframe.cpp

run-tests:
	-./tests/testKey
	-./tests/testKVStore
	-./tests/testValue
	-./tests/testSchema
	-./tests/testRow
	-./tests/testColumn
	-./tests/testDataframe

clean-tests:
	-cd ./tests; rm testKey testKVStore testValue testSchema testRow testColumn testDataframe

clean:
	-rm -r main *.dSYM