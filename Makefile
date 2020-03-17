pull: pull-1 pull-2 pull-3 pull-4 pull-5 pull-6

pull-1:
	cd ./a1/part1; git pull
	cd ./a1/part2; git pull
	echo "a1 up to date."

pull-2:
	echo "a2 up to date."

pull-3:
	cd ./a3; git pull
	echo "a3 up to date."

pull-4:
	cd ./a4; git pull
	echo "a4 up to date."

pull-5:
	cd ./a5; git pull
	echo "a5 up to date."

pull-6:
	cd ./a6; git pull
	echo "a6 up to date."