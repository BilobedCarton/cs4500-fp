
build: buildArray buildQueue buildMap

test: testArray testQueue testMap clean

testArray: buildArray
	-./testArr

testQueue: buildQueue
	-./testQue

testMap: buildMap
	-./testMap

buildArray:
	g++ -o testArr -g -Wall -std=c++11 ./test-array.cpp

buildQueue:
	g++ -o testQue -g -Wall -std=c++11 ./test-queue.cpp

buildMap:
	g++ -o testMap -g -Wall -std=c++11 ./test-map.cpp

clean: clean_supp clean_exec

clean_exec:
	-rm testArr testQue testMap

clean_supp:
	-rm -r *.dSYM
