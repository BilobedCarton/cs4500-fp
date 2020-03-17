//lang::CwC
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
#include "utils/array.h"
#include "utils/object.h"
#include "message.h"
#include "utils/thread.h"

static const size_t MSG_BUFFER_SIZE = 1024; // currently we only handle 1024 character messages

/**
 * @brief      This class describes a client used to connect to a server.
 */
class Client : public Peer {
public:
    Array* _contacts; // owned - contacts owned
    bool _receivedShutdown; // did we receive a shutdown command from the server?

    /**
     * @brief      Constructs a new instance.
     *
     * @param      ip    our ip
     */
    Client(String* ip, char* server_ip) : Peer(ip) {
        // try to connect to the server
        struct sockaddr_in server;
        server.sin_family = AF_INET;
        server.sin_addr.s_addr = inet_addr(server_ip);
        server.sin_port = htons(ACTIVE_PORT);

        if (connect(_socket, (struct sockaddr *)&server, sizeof(server)) != 0) { 
            printf("Failed to connect.\n"); 
            assert(false);
        } else {
            printf("Successfully connected!\n");
        }

        _contacts = new Array();
        _receivedShutdown = false;
    }

    /**
     * @brief      Destroys the object.
     */
    ~Client() {
        for (int i = 0; i < _contacts->count(); ++i)
        {
            delete(_contacts->get(i));
        }
        delete(_contacts);
    }

    /**
     * @brief      Sends a message to the server.
     *
     * @param      m     { parameter_description }
     */
    void SendMessage(Message* m) {
        SendMessageThroughSocket(m, _socket);
    }

    /**
     * @brief      Sends a status.
     */
    void SendStatus(char* status, size_t target) {
        String* msg = new String(status);
        Status* s = new Status(msg, _id, target);
        SendMessage(s);
        delete(msg);
        delete(s);
    }

    /**
     * @brief      Sends a shutdown command to the server.
     */
    void SendShutdown() {
        Message* m = new Message(7, _id, 0);
        SendMessage(m);
        delete(m);
    }

    /**
     * @brief      Update our list of contacts based on the given directory message
     *
     * @param      d     the directory message received from the server
     */
    void UpdatePeerList(Directory* d) {
        // purge our directory
        for (int i = 0; i < _contacts->count(); ++i)
        {
            delete(_contacts->get(i));
        }
        delete(_contacts);
        
        _contacts = new Array();
        printf("Peer list updated: \n");
        for (int i = 0; i < d->clients; ++i)
        {
            Contact* c = new Contact(d->ids[i], d->addresses->get(i));
            _contacts->append(c);
            printf("%zu : %s\n", c->_id, c->_ip->c_str());
        }
    }

    /**
     * @brief      Request a directory update from the server.
     */
    void RequestDirectory() {
        Message* m = new Message(4, _id, 0);
        SendMessage(m);
        delete(m);
    }

    /**
     * @brief      Send a disconnect notice to the server and then close our socket.
     */
    void Disconnect() {
        printf("Disconnecting from server. \n");
        Message* m = new Message(MsgKind::Disconnect, _id, 0);
        SendMessage(m);
        delete(m);
    } 

    /**
     * @brief      Shutdown this Client program. (Might become handled externally?)
     */
    void Shutdown() {
        close(_socket);
        _receivedShutdown = true;
    }

    /**
     * @brief      Properly handle a received message based on the MsgKind
     *
     * @param      m     the message
     */
    void HandleMessage(Message *m) {
        Status* s = nullptr;
        Directory* d = nullptr;

        switch(m->kind_) {
            case MsgKind::Ack:
                _id = m->target_;
                printf("Our id is %zu\n", _id);
                delete(m);
                return;
            case MsgKind::Status:
                s = dynamic_cast<Status *>(m);
                assert(s != nullptr);
                printf("%s\n", s->msg_->c_str());
                delete(s);
                return;
            case MsgKind::Kill:
                delete(m);
                printf("Received shutdown signal. If hanging, please enter anything.\n");
                Shutdown();
                return;
            case MsgKind::Directory:
                d = dynamic_cast<Directory *>(m);
                assert(d != nullptr);
                UpdatePeerList(d);
                delete(d);
                return;
            default:
                delete(m);
                assert(false);
                return;
        }
    }
};