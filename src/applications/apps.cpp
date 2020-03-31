#include "demo.h"

int main() {
    Demo prod(0);
    Demo count(1);
    Demo sum(2);

    // prod.run_();
    // count.run_();
    // sum.run_();

    NodeThread prodThread(&prod);
    NodeThread countThread(&count);
    NodeThread sumThread(&sum);

    Sys s;

    s.pln("Starting prod thread.");
    prodThread.start();
    s.pln("Starting count thread.");
    countThread.start();
    s.pln("Starting sum thread.");
    sumThread.start();

    s.pln("Joining threads");

    prodThread.join();
    countThread.join();
    sumThread.join();
}