#include "demo.h"
#include "../utils/args.h"

int main(int argc, char** argv) {
    CreateArgs(argc, argv);

    NetworkIP* net = new NetworkIP();
    net->register_node(args->idx);
    Demo d(args->idx, net);

    //d.kv.network_->register_node(args->idx);
    d.p("Starting node: ").pln(d.idx_);
    d.run_();
    while(true) {

    }
}