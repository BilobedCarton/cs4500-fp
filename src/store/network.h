#pragma once

#include <assert.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "../utils/object.h"
#include "../utils/string.h"
#include "../utils/array.h"
#include "../utils/thread.h"
#include "../utils/map.h"
#include "../utils/args.h"
#include "../utils/logger.h"
#include "message.h"

class MsgQue : public Object {
public:
    Array* arr_;
    Lock lock_;

    MsgQue() {
        arr_ = new Array();
    }

    bool hasMessages() { return arr_->count() > 0; }

    void push(Message* m) {
        lock_.lock();
        arr_->append(m); 
        lock_.unlock();
    }

    Message* pop() {
        lock_.lock();
        Message* m;
        if(arr_->count() == 0) m = nullptr; 
        else m = dynamic_cast<Message *>(arr_->pop(arr_->count() - 1)); 
        lock_.unlock();
        return m;
    }

    Object* clone() {
        return this;
    }
};

class Size_t : public Object {
public:
    size_t value_;

    Size_t(size_t value) {
        value_ = value;
    }

    Object* clone() { return new Size_t(value_); }
};

class MsgQueArr : public Array {
public:
    MsgQueArr(size_t cap) : Array(cap) {}
    MsgQue* get(size_t idx) { return dynamic_cast<MsgQue*>(Array::get(idx)); }
};

class StringSize_tMap : public Map {
public:
    Lock lock_;

    void put(String* k, size_t v) {
        lock_.lock();
        Size_t* v_obj = new Size_t(v);
        Map::put(k, v_obj);
        Logger::log_register(v, k->c_str());
        delete(v_obj);
        lock_.unlock();
    }

    size_t get(String* k) {
        lock_.lock();
        Size_t* v = (Size_t *)Map::get(k);
        if(v == nullptr) {
            p("Could not find node for: ").pln(k->c_str());
        }
        size_t t = v->value_;
        lock_.unlock();
        return t;
    }
};

class NetworkIfc : public Object {
public:

    virtual void register_node(size_t idx) { return; }

    virtual size_t index() { assert(false); }

    virtual void send_message(Message* msg) = 0;

    virtual Message* receive_message() = 0;
};

class PseudoNetwork : public NetworkIfc {
public:
    MsgQueArr msgques_;
    StringSize_tMap threads_;

    PseudoNetwork(size_t num_nodes) : msgques_(num_nodes) {
        for (size_t i = 0; i < num_nodes; i++)
        {
            msgques_.append(new MsgQue());
        }
    }

    void register_node(size_t idx) {
        String* tid = Thread::thread_id();
        threads_.put(tid, idx);
        delete(tid);
    }

    void send_message(Message* msg) {
        Logger::log_send(msg);
        msgques_.get(msg->target_)->push(msg); 
    }

    Message* receive_message() {
        String* tid = Thread::thread_id();
        if(tid == nullptr) {
            return nullptr;
        }
        size_t idx = threads_.get(tid);
        delete(tid);
        MsgQue* que = msgques_.get(idx);
        if(!que->hasMessages()) return nullptr;
        Message* m = que->pop();
        Logger::log_receive(m);
        return m;
    }

    // return this because we're faking the network
    Object* clone() {
        return this;
    }
};

class NodeInfo : public Object {
public:
    unsigned id;
    sockaddr_in address;
};

class NetworkIP : public NetworkIfc {
public:
    NodeInfo* nodes_; // all nodes
    size_t this_node_; // our index
    int sock_; // our socket
    sockaddr_in ip_; // our ip

    ~NetworkIP() { close(sock_); }

    size_t index() { return this_node_; }

    void server_init(unsigned idx, unsigned port, size_t num_nodes) {
        this_node_ = idx;
        init_sock_(port);

        // set up node list
        nodes_ = new NodeInfo[num_nodes];
        for (size_t i = 0; i < num_nodes; i++)
        {
            nodes_[i].id = 0;
        }
        nodes_[0].address = ip_;
        nodes_[0].id = 0;

        // register all nodes
        for (size_t i = 1; i < num_nodes; i++)
        {
            Register* msg = dynamic_cast<Register*>(receive_message());
            nodes_[msg->sender_].id = msg->sender_;
            nodes_[msg->sender_].address.sin_family = AF_INET;
            nodes_[msg->sender_].address.sin_addr = msg->client.sin_addr;
            nodes_[msg->sender_].address.sin_port = htons(msg->port);
            delete(msg);
        }

        // set up info for directories
        size_t* ports = new size_t[num_nodes - 1];
        String** addresses = new String*[num_nodes - 1];
        for (size_t i = 0; i < num_nodes - 1; i++)
        {
            ports[i] = ntohs(nodes_[i + 1].address.sin_port);
            addresses[i] = new String(inet_ntoa(nodes_[i + 1].address.sin_addr));
        }

        // send out directories
        for (size_t i = 0; i < num_nodes; i++)
        {
            Directory* ipd = new Directory(num_nodes - 1, ports, addresses);
            ipd->target_ = i;
            send_message(ipd);
        }
    }

    void client_init(unsigned idx, unsigned port, char* server_adr, unsigned server_port, size_t num_nodes) {
        this_node_ = idx;
        init_sock_(port);

        // set up node list
        nodes_ = new NodeInfo[1];
        nodes_[0].id = 0;
        nodes_[0].address.sin_family = AF_INET;
        nodes_[0].address.sin_port = htons(server_port);

        if(inet_pton(AF_INET, server_adr, &nodes_[0].address.sin_addr) <= 0) assert(false && "Invalid server IP address format");
        
        // set up ip
        char* ip_arr = new char[INET_ADDRSTRLEN + 1];
        ip_arr[INET_ADDRSTRLEN] = '\0';
        inet_ntop(AF_INET, &ip_.sin_addr, ip_arr, INET_ADDRSTRLEN);
        String* ip = new String(ip_arr);

        // register with server
        Register* msg = new Register(*ip, port);
        send_message(msg);

        // handle directory
        Directory* ipd = dynamic_cast<Directory*>(receive_message());
        NodeInfo* nodes = new NodeInfo[num_nodes];
        nodes[0] = nodes_[0];
        for (size_t i = 0; i < ipd->num_nodes_; i++)
        {
            nodes[i + 1].id = i + 1;
            nodes[i + 1].address.sin_family = AF_INET;
            nodes[i + 1].address.sin_port = htons(ipd->ports_[i]);
            if(inet_pton(AF_INET, ipd->addresses_[i]->c_str(), &nodes[i + 1].address.sin_addr) <= 0) {
                p("Invalid IP directory-addr. for node ").pln(i + 1);
                assert(false);
            }
        }
        delete[](nodes_);
        nodes_ = nodes;
        delete ipd;
    }

    void init_sock_(unsigned port) {
        assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
        int opt = 1;
        assert(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, /* | SO_REUSEPORT */ &opt, sizeof(opt)) == 0);

        // set up ip
        ip_.sin_family = AF_INET;
        ip_.sin_addr.s_addr = INADDR_ANY;
        ip_.sin_port = htons(port);
        p("Using address: ").pln(inet_ntoa(ip_.sin_addr));

        if(bind(sock_, (sockaddr*)&ip_, sizeof(sockaddr)) < 0) {
            p("Bind failed: errno - ").pln(errno);
            assert(false);
        }
        assert(listen(sock_, 100) >= 0); // connections queue size
    }

    void register_node(size_t idx) {
        assert(args != nullptr);
        if(idx == 0) server_init(idx, args->port, args->num_nodes);
        else client_init(idx, args->port, args->server_adr, args->server_port, args->num_nodes);
    }

    void send_message(Message* msg) {
        msg->sender_ = index();
        NodeInfo& tgt = nodes_[msg->target_];

        int conn = socket(AF_INET, SOCK_STREAM, 0);
        assert(conn >= 0 && "Unable to create client socket");
        if(connect(conn, (sockaddr*)&tgt.address, sizeof(sockaddr)) < 0) {
            perror("Unable to connect to remote node");
            assert(false);
        } 

        Logger::log_send(msg);
        SerialString* ss = msg->serialize();
        send(conn, &ss->size_, sizeof(size_t), 0);
        send(conn, ss->data_, ss->size_, 0);
        delete(ss);
        delete(msg);
    }

    Message* receive_message() {
        sockaddr_in sender;
        socklen_t addrlen = sizeof(sender);
        int req = accept(sock_, (sockaddr*)&sender, &addrlen);
        size_t size = 0;
        if(read(req, &size, sizeof(size_t)) == 0) assert(false && "Failed to read");
        char* buf = new char[size];
        int rd = 0;
        while(rd != size) rd += read(req, buf + rd, size - rd);
        SerialString* ss = new SerialString(buf, size);
        delete[](buf);
        Message* msg = msg_deserialize(ss);
        if(msg == nullptr) return nullptr;

        Logger::log_receive(msg);
        delete(ss);
        return msg;
    }
};

