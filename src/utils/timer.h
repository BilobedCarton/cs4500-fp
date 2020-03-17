//lang::CwC

#pragma once

#include <time.h>

class Timer {
public:
	struct timespec _start;
	double _elapsed;

	Timer() {
		_elapsed = -1;
	}

	void start() {
		clock_gettime(CLOCK_MONOTONIC, &_start);
	}

	void stop() {
		if(_elapsed != -1) { return; } // already stopped
		struct timespec end;
		clock_gettime(CLOCK_MONOTONIC, &end);
		_elapsed = end.tv_sec - _start.tv_sec;
		_elapsed += (end.tv_nsec - _start.tv_nsec) /  1000000000.0;
	}

	void resume() {
		_elapsed = -1;
	}

	void restart() {
		resume();
		start();
	}

	double get_time_elapsed() {
		return _elapsed * 1000; // milliseconds
	}
};