run: build test clean

build: main

test:
	-./main -f ../tests/allTypes.sor -op PRINT
	-./main -f ../tests/data.sor -op TYPE -col 2
	-./main -f ../tests/allTypes.sor -op GET -col 3 -row 2

main:
	g++ -o main -g -Wall -std=c++11 ./main.cpp

valgrind:
	valgrind --leak-check=yes --track-origins=yes ./main.cpp

clean:
	-rm -r main *.dSYM