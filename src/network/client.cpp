//lang::CwC

#include <iostream>

#include "network.h"
#include "thread.h"

/**
 * @brief      This class describes a client listener to receive messages.
 */
class ClientListener : public Thread {
public:
    Client* c;
    bool active;

    /**
     * @brief      Constructs a new instance.
     *
     * @param      client  The client we're listening to
     */
    ClientListener(Client* client) {
        c = client;
    }

    void run() { 
        active = true;
        while(active) {    
            c->ReceiveMessage(c->_socket);
            sleep(5);
        }
    }
};

int main(int argc, char** argv) {
    if(argc != 3) {
        printf("Arguments invalid, proper usage: %s [our ip] [server ip]\n", argv[0]);
        return 1;
    }
    char* server_ip = argv[2];
    Client* c = new Client(new String(argv[1]), server_ip);
    c->ReceiveMessage(); // wait for ack
    c->RequestDirectory(); // request directory
    
    // set up listener
    ClientListener* cl = new ClientListener(c);
    cl->start();

    Sys s;
    // enter command interface
    while(true) {
    	s.pln("Please input a command:")
    		.pln("1. Get Directory Update.")
    		.pln("2. Send a Message.")
    		.pln("3. Send a Shutdown Command.")
    		.pln("4. Quit.");
    	
        // used for inputs
        size_t cmd;
    	char* message = new char[1000];
    	size_t target;
        char ch;

    	std::cin >> cmd;

        // did our client receive a shutdown command from the server?
        if(c->_receivedShutdown) {
            s.pln("Connection closed. Closing client.");
            cl->active = false;
            delete(c);
            return 0;
        }

        // switch based on received command
    	switch(cmd) {
    		case 1: // directory request
    			c->RequestDirectory();
    			break;
    		case 2: // send message
    			s.pln("Enter a message. ");

                while((ch = getchar()) != '\n' && ch != EOF); // purge input of unhelpful chars to allow for fgets
				fgets(message, 800, stdin);
				message[strlen(message) - 1] = '\0';

    			s.pln("Enter the id of the target.");
    			std::cin >> target;

    			c->SendStatus(message, target);
    			break;
    		case 3: // send shutdown to server
    			c->SendShutdown();
    			break;
    		case 4: // shutdown ourself
    			cl->active = false; // turn off listener
                c->Disconnect(); // tell server we're shutting down
                c->ReceiveMessage(); // listen for response
    			//if(!c->_receivedShutdown) c->Shutdown(); // disconnect if we haven't already
				delete(c);
    			return 0;
    		default:
    			s.pln("Invalid command entered. ");
    			break;
    	}
    }

}