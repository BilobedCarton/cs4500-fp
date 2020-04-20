temp:
	cd ./tests; g++ -o testDistributedColumn.bin -Wall -g -std=c++17 ./dataframe/testDistributedColumn.cpp
	lldb ./tests/testDistributedColumn.bin

clean-temp:
	rm -r ./tests/testDistributedColumn.bin ./tests/testDistributedColumn.bin.dSYM

build-demo:
	g++ -o pseudo_demo -Wall -std=c++17 ./src/applications/pseudo_demo.cpp
	g++ -o networked_demo -Wall -std=c++17 ./src/applications/networked_demo.cpp

run-pseudo-demo:
	./pseudo_demo

net-demo-0:
	./networked_demo -nn 3 -p 1025 -idx 0

net-demo-1:
	./networked_demo -nn 3 -p 1026 -idx 1 -sp 1025 -sa 0.0.0.0

net-demo-2:
	./networked_demo -nn 3 -p 1027 -idx 2 -sp 1025 -sa 0.0.0.0

clean-demo:
	rm pseudo_demo networked_demo

valgrind:
	valgrind --leak-check=yes --track-origins=yes ./main.cpp

test: build-tests run-tests clean-tests

build-tests:
	cd ./tests; g++ -o testArray.bin -Wall -std=c++17 ./utils/testArray.cpp
	cd ./tests; g++ -o testMap.bin -Wall -std=c++17 ./utils/testMap.cpp
	cd ./tests; g++ -o testPrimitiveArray.bin -Wall -std=c++17 ./utils/testPrimitiveArray.cpp
	cd ./tests; g++ -o testKey.bin -Wall -std=c++17 ./store/testKey.cpp
	cd ./tests; g++ -o testValue.bin -Wall -std=c++17 ./store/testValue.cpp
	cd ./tests; g++ -o testMessage.bin -Wall -std=c++17 ./store/testMessage.cpp
	cd ./tests; g++ -o testNetwork.bin -Wall -std=c++17 ./store/testNetwork.cpp
	cd ./tests; g++ -o testKVStore.bin -Wall -std=c++17 ./store/testKVStore.cpp
	cd ./tests; g++ -o testSchema.bin -Wall -std=c++17 ./dataframe/testSchema.cpp
	cd ./tests; g++ -o testRow.bin -Wall -std=c++17 ./dataframe/testRow.cpp
	cd ./tests; g++ -o testColumn.bin -Wall -std=c++17 ./dataframe/testColumn.cpp
	cd ./tests; g++ -o testDataframe.bin -Wall -std=c++17 ./dataframe/testDataframe.cpp
	cd ./tests; g++ -o testDistributedColumn -Wall -std=c++17 ./dataframe/testDistributedColumn.cpp
	cd ./tests; g++ -o testMessage.bin -Wall -std=c++17 ./store/testMessage.cpp

run-tests:
	-./tests/testArray.bin; echo
	-./tests/testMap.bin; echo
	-./tests/testPrimitiveArray.bin; echo
	-./tests/testKey.bin; echo
	-./tests/testValue.bin; echo
	-./tests/testMessage.bin; echo
	-./tests/testNetwork.bin; echo
	-./tests/testKVStore.bin; echo
	-./tests/testSchema.bin; echo
	-./tests/testRow.bin; echo
	-./tests/testColumn.bin; echo
	-./tests/testDataframe.bin; echo
	-./tests/testDistributedColumn.bin; echo

clean-tests:
	-cd ./tests; rm *.bin

clean:
	-rm -r main *.dSYM