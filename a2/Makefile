ARRAYSPECREPO = https://github.com/BilobedCarton/a1p2.git # forked repository
ARRAYSPECHASH = 416cb4956f1d0cd0966eef34dbf68fecfde25b2c
QUEUESPECREPO = https://github.com/hakjoonlee/CS4500-A1P2.git
QUEUESPECHASH = 978d8b43d328979cb373971e6705e735455f6bf2
MAPSPECREPO = https://github.com/TomPeng19971208/CS4500_A1_Part2.git
MAPSPECHASH = 7477b926081326c62340550ad5b9458f1e99256b

run: dircheck
	- make first
	- make second
	- make third

first:
	docker build -t cs4500:0.1 . 
	- rm -rf ./part1/test-array
	git clone $(ARRAYSPECREPO) ./part1/test-array
	cd ./part1/test-array; git checkout $(ARRAYSPECHASH)
	cd ./part1; cp array.h queue.h map.h string.h object.h Makefile ./test-array 
#	TODO: cp anything else to compile and run e.g. googletests? my-tests.cpp edited-tests.cpp 
	- docker run -ti -v `pwd`:/test cs4500:0.1 bash -c "cd /test/part1/test-array ; make testArray"

second:
	docker build -t cs4500:0.1 . 
	- rm -rf ./part1/test-queue
	git clone $(QUEUESPECREPO) ./part1/test-queue
	cd ./part1/test-queue; git checkout $(QUEUESPECHASH)
	cd ./part1; cp array.h queue.h map.h string.h object.h Makefile ./test-queue 
#	TODO: cp anything else to compile and run e.g. googletests? my-tests.cpp edited-tests.cpp 
	- docker run -ti -v `pwd`:/test cs4500:0.1 bash -c "cd /test/part1/test-queue ; make testQueue"

third:
	docker build -t cs4500:0.1 . 
	- rm -rf ./part1/test-map
	git clone $(MAPSPECREPO) ./part1/test-map
	cd ./part1/test-map; git checkout $(MAPSPECHASH)
	cd ./part1; cp array.h queue.h map.h string.h object.h Makefile ./test-map
#	TODO: cp anything else to compile and run e.g. googletests? my-tests.cpp edited-tests.cpp 
	- docker run -ti -v `pwd`:/test cs4500:0.1 bash -c "cd /test/part1/test-map ; make testMap"

dircheck: ./part1 ./part1/array.h ./part1/queue.h ./part1/map.h ./part1/string.h ./part1/object.h ./part1/Makefile ./part1/MEMO.md ./part2 ./part2/REPO.md
	echo "Directory structure looks OK"
	- rm -rf dirtest-dir
	wc -l < ./part2/REPO.md | xargs test 2 -gt 
	echo "REPO.md file is 0 or 1 lines"
# B/c text files should always end w/a newline: stackoverflow.com/questions/729692/why-should-text-files-end-with-a-newline
	cat ./part2/REPO.md | xargs -n 1 -I{} git clone -q {} dirtest-dir
	echo "REPO.md file contains a valid git clone URL"
	rm -rf dirtest-dir
