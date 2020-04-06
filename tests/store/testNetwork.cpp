
#include <assert.h>

#include "../../src/store/network.h"
#include "../../src/utils/thread.h"
#include "../test.h"

class NetThread : public Thread {
public:
    NetworkIfc* net_;
    size_t idx_;
    Message* send_; // owned
    Message* received_; // not owned
    size_t state_;

    NetThread(NetworkIfc* net, size_t idx) {
        net_ = net;
        idx_ = idx;
        state_ = 0;
    }

    ~NetThread() {
        delete(send_);
    }

    void set_state(size_t s) { state_ = s; }
    void set_send(Message* m) { send_ = m; }
    Message* get_received() { return received_; }

    void run() {
        while(state_ < 4) {
            if(state_ == 1) {
                net_->register_node(idx_);
                state_ = 0;
            }
            else if(state_ == 2) {
                net_->send_message(send_);
                state_ = 0;
            }
            else if(state_ == 3) {
                received_ = net_->receive_message();
                state_ = 0;
            }
        }
    }
};

class TestPseudoNetwork : public Test {
public:
    PseudoNetwork net;
    NetThread** threads;
    Message** msgs;

    TestPseudoNetwork() : net(4) {
        threads = new NetThread*[4];
        msgs = new Message*[4];
        for (size_t i = 0; i < 4; i++)
        {
            Key k("test", i); // just send the message to ourself
            msgs[i] = new Get(&k);
            threads[i] = new NetThread(&net, i);
            threads[i]->start();
        }
    }

    ~TestPseudoNetwork() {
        for (size_t i = 0; i < 4; i++)
        {
            threads[i]->set_state(4);
            threads[i]->join();
            delete(threads[i]);
        }
        
        delete[](threads);
        delete[](msgs);
    }

    bool testRegister() {
        for (size_t i = 0; i < 4; i++)
        {
            threads[i]->set_state(1);
        }
        sleep(1);
        assert(net.threads_.count() == 4);

        OK("PseudoNetwork::register_node(idx) -- passed.");
        return true;
    }

    bool testSend() {
        for (size_t i = 0; i < 4; i++)
        {
            threads[i]->set_send(msgs[i]);
            threads[i]->set_state(2);
        }
        sleep(1);
        for (size_t i = 0; i < 4; i++)
        {
            assert(net.msgques_.get(i)->arr_->count() == 1);
        }

        OK("PseudoNetwork::send_message(msg) -- passed.");
        return true;
    }

    bool testReceive() {
        for (size_t i = 0; i < 4; i++)
        {
            threads[i]->set_state(3);
        }
        sleep(1);
        for (size_t i = 0; i < 4; i++)
        {
            assert(threads[i]->send_->equals(threads[i]->get_received()));
        }

        OK("PseudoNetwork::receive_message() -- passed.");
        return true;
    }

    bool run() {
        return testRegister() && testSend() && testReceive();
    }
};

int main() {
    TestPseudoNetwork pseudo;
    pseudo.testSuccess();
}