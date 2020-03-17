//lang::CwC

#include "network.h"
#include "utils/thread.h"

class ServerRegisterListener : public Thread {
public:
	Server* _s;
	bool _active;

	ServerRegisterListener(Server* s) {
		_s = s;
		_active = true;
	}

    void run() { 
		while(_active) {    
	    	_s->RegisterNode(); // check for incoming connections
	    	sleep(5);
	    }
    }
};

int main(int argc, char** argv) {
	if(argc != 2) {
		printf("Arguments invalid, proper usage: %s [ip]\n", argv[0]);
		return 1;
	}
    Server* serv = new Server(new String(argv[1]));

    ServerRegisterListener* servRegList = new ServerRegisterListener(serv);
    servRegList->start();

    Sys s;
    size_t in;
    s.pln("Enter anything to quit");
    std::cin >> in;
    s.pln("Closing server.");

    servRegList->_active = false;

    if(!serv->_receivedShutdown) serv->Shutdown();
    servRegList->join();

	delete(serv);
}