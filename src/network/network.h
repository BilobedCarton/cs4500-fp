#pragma once

#include <unistd.h> 
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h> 
#include <stdlib.h> 
#include <netinet/in.h> 
#include <string.h>
#include <assert.h>
#include <arpa/inet.h> // Standards and tutorials across the web all use this library
#include <iostream>
#include <fcntl.h>
#include <errno.h>

#include "network.h"
#include "array.h"
#include "object.h"
#include "message.h"
#include "thread.h"

static const int ACTIVE_PORT = 8080;
static const char* HOST_SERVER = "127.0.0.1";

/**
 * @brief      This class describes a contact to describe contacts in the system.
 */
class Contact : public Object {
public:
    size_t _id; // id in the server system
    String* _ip; // ip address

    /**
     * @brief      Constructs a new instance.
     *
     * @param[in]  id    The identifier
     * @param      ip    ip address
     */
    Contact(size_t id, String* ip) {
        _id = id;
        _ip = new String(*ip);
    }

    /**
     * @brief      Destroys the object.
     */
    ~Contact() {
        delete(_ip);
    }
};

/**
 * @brief      This class describes a peer, a class desribing a member of the network.
 *             Both Clients and Servers are Peers
 */
class Peer : public Contact {
public:
    int _socket; // how we communicate with the network

    /**
     * @brief      Constructs a new instance.
     *
     * @param      ip    our ip address
     */
    Peer(String* ip) : Contact(0, ip) {
        _socket = socket(AF_INET, SOCK_STREAM, 0); // IPv4, TCP, Internet protocol
        assert(_socket != 0); // failed to create?

        struct sockaddr_in addr;

        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(ip->c_str());
        addr.sin_port = htons(ACTIVE_PORT);

        assert(bind(_socket, (sockaddr *)&addr, sizeof(addr)) >= 0); // bind and fail if unsuccessful
    }

    /**
     * @brief      Destroys the object.
     */
    ~Peer() { }

    virtual void HandleMessage(Message* m);
    virtual void SendMessage(Message* m);

    /**
     * @brief      Sends a message to the message target.
     *
     * @param      m     the message
     */
    void SendMessageThroughSocket(Message* m, int socket) {
        char* serialized_msg = m->serialize();
        printf("SENDING MESSAGE: %s\n", serialized_msg);
        send(socket, serialized_msg, strlen(serialized_msg), 0);
        delete[](serialized_msg);
    }

    /**
     * @brief      Parse the given string and return the proper Message object
     *
     * @param      message  The message
     *
     * @return     the deserialized message
     */
    Message* ParseMessage(char* message) {
        String* m_copy = new String(message);
        Message* m = Message::deserialize(m_copy->c_str());
        
        switch(m->kind_) {
            case MsgKind::Status:
                delete(m);
                m = Status::deserialize(message);
                break;
            case MsgKind::Register:
                delete(m);
                m = Register::deserialize(message);
                break;
            case MsgKind::Directory:
                delete(m);
                m = Directory::deserialize(message);
                break;
            default:
                break;
        }

        delete(m_copy);
        return m;
    }

    /**
     * @brief      Listen for a message from the given node
     *
     * @param[in]  id    The node identifier
     */
    void ReceiveMessage(int socket) {
        char* message = new char[MSG_BUFFER_SIZE];
        ssize_t recieved = read(socket, message, MSG_BUFFER_SIZE);
        
        if (recieved < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) return; // didn't read anything from the socket
            else if(errno == EBADF) return; // socket was closed  
            assert(false); // big error
        } 
        else if(recieved == 0) return; // we don't have anything to read
        
        printf("RECIEVED MSG: %s\n", message);

        HandleMessage(ParseMessage(message));
    }
};

