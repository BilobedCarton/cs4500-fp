#include "demo.h"

int main() {
    PseudoNetwork* net = new PseudoNetwork(3);

    Demo prod(0, net);
    Demo count(1, net);
    Demo sum(2, net);

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