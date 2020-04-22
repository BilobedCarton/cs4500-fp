#include "demo.h"

int main() {
    args = new Args();
    args->num_nodes = 3;

    PseudoNetwork* net = new PseudoNetwork(3);

    Demo* prod = new Demo(0, net);
    Demo* count = new Demo(1, net);
    Demo* sum = new Demo(2, net);

    NodeThread prodThread(prod);
    NodeThread countThread(count);
    NodeThread sumThread(sum);

    Logger::log("Starting prod thread.");
    prodThread.start();
    Logger::log("Starting count thread.");
    countThread.start();
    Logger::log("Starting sum thread.");
    sumThread.start();

    Logger::log("Joining threads");

    prodThread.join();
    countThread.join();
    sumThread.join();
}