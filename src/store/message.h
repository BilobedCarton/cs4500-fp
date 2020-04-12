#pragma once

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "../utils/serial.h"
#include "../utils/string.h"
#include "../utils/thread.h"
#include "key.h"
#include "value.h"

enum class MsgType { Register = 0, Get, Put, Status, Directory, Fail };

class Message : public SerializableObject {
public:
    MsgType type_;
    size_t target_;
    size_t sender_; // set by network on send

    Message(Message& m) {
        type_ = m.type_;
        target_ = m.target_;
        sender_ = m.sender_;
    }

    Message(MsgType type, size_t target) {
        type_ = type;
        target_ = target;
    }

    Message(size_t type, size_t target) : Message(static_cast<MsgType>(type), target) { }

    SerialString* serialize() {
        char* arr = new char[3 * sizeof(size_t)];
        size_t type = static_cast<size_t>(type_);
        memcpy(arr, &type, sizeof(size_t));
        memcpy(arr + sizeof(size_t), &target_, sizeof(size_t));
        memcpy(arr + (2 *sizeof(size_t)), &sender_, sizeof(size_t));

        SerialString* ss = new SerialString(arr, 3 * sizeof(size_t));
        delete[](arr);
        return ss;
    }

    static Message* deserialize_(SerialString* serial) {
        size_t type;
        size_t target;
        size_t sender;

        memcpy(&type, serial->data_, sizeof(size_t));
        memcpy(&target, serial->data_ + sizeof(size_t), sizeof(size_t));
        memcpy(&sender, serial->data_ + (2 * sizeof(size_t)), sizeof(size_t));

        Message* m = new Message(type, target);
        m->sender_ = sender;
        return m;
    }

    bool equals(Object* other) {
        Message* cast = dynamic_cast<Message *>(other);
        if(cast == nullptr) return false;
        return (type_ == cast->type_ && target_ == cast->target_ && sender_ == cast->sender_);
    }
};

class Register : public Message {
public:
    sockaddr_in client;
    size_t port;

    Register(Message& m, sockaddr_in c, size_t p) : Message(m) {
        memcpy(&client, &c, sizeof(sockaddr_in));
        port = p;
    }

    Register(String& ip, size_t p) : Message(MsgType::Register, 0) {
        client.sin_family = AF_INET;
        client.sin_port = htons(p);
        inet_aton(ip.c_str(), &client.sin_addr);

        port = p;
    } 

    SerialString* serialize() {
        SerialString* m_ss = Message::serialize();
        char* arr = new char[m_ss->size_ + sizeof(sockaddr_in) + sizeof(size_t)];
        size_t pos = 0;

        memcpy(arr + pos, m_ss->data_, m_ss->size_);
        pos += m_ss->size_;
        delete(m_ss);

        memcpy(arr + pos, &client, sizeof(sockaddr_in));
        pos += sizeof(sockaddr_in);

        memcpy(arr + pos, &port, sizeof(size_t));
        pos += sizeof(size_t);

        SerialString* ss = new SerialString(arr, pos);
        delete[](arr);
        return ss;
    }

    static Register* deserialize(SerialString* string) {
        Message* m = Message::deserialize_(string);

        SerialString* substr = new SerialString(string->data_ + (3 * sizeof(size_t)), string->size_ - (3 * sizeof(size_t)));
        sockaddr_in c;
        size_t p;

        memcpy(&c, substr->data_, sizeof(sockaddr_in));
        memcpy(&p, substr->data_ + sizeof(sockaddr_in), sizeof(size_t));
        delete(substr);

        Register* r = new Register(*m, c, p);
        delete(m);
        return r;
    }

    bool equals(Object* other) {
        if(!Message::equals(other)) return false;
        Register* cast = dynamic_cast<Register *>(other);
        if(cast == nullptr) return false;
        return (client.sin_addr.s_addr == cast->client.sin_addr.s_addr 
             && client.sin_port == cast->client.sin_port
             && port == cast->port);
    }
};

class Get : public Message {
public:
    Key* k_; // owned

    Get(Message& m, Key& k) : Message(m) {
        k_ = dynamic_cast<Key *>(k.clone());
    }

    Get(Key* k) : Message(MsgType::Get, k->idx_) {
        k_ = dynamic_cast<Key *>(k->clone());
    }

    ~Get() { delete(k_); }

    virtual SerialString* serialize() {
        SerialString* m_ss = Message::serialize();
        SerialString* k_ss = k_->serialize();

        size_t size = m_ss->size_ + k_ss->size_;
        char* arr = new char[size];
        memcpy(arr, m_ss->data_, m_ss->size_);
        memcpy(arr + m_ss->size_, k_ss->data_, k_ss->size_);

        delete(m_ss);
        delete(k_ss);

        SerialString* ss = new SerialString(arr, size);
        delete[](arr);

        return ss;
    }

    static Get* deserialize(SerialString* string) {
        Message* m = Message::deserialize_(string);

        SerialString* key_substr = new SerialString(string->data_ + (3 * sizeof(size_t)), string->size_ - (3 * sizeof(size_t)));
        Key* k = Key::deserialize(key_substr);
        delete(key_substr);

        Get* g = new Get(*m, *k);

        delete(m);
        delete(k);
        return g;
    }

    bool equals(Object* other) {
        if(!Message::equals(other)) return false;
        Get* cast = dynamic_cast<Get *>(other);
        if(cast == nullptr) return false;
        return k_->equals(cast->k_);
    }
};

class Fail : public Get {
public:

    Fail(Key* k) : Get(k) {
        type_ = MsgType::Fail;
    }

    static Fail* deserialize(SerialString* string) {
        Get* g = Get::deserialize(string);
        Fail* f = new Fail(g->k_);
        f->target_ = g->target_;
        f->sender_ = g->sender_;
        f->type_ = MsgType::Fail;
        return f;
    }
};

class Put : public Get {
public:
    Value* v_; // owned

    Put(Message& m, Key& k, Value& v) : Get(m, k) {
        v_ = v.clone();
        type_ = MsgType::Put;
    }

    Put(Key* k, Value* v) : Get(k) {
        v_ = v->clone();
        type_ = MsgType::Put;
    }

    ~Put() { delete(v_); }

    SerialString* serialize() {
        SerialString* g_ss = Get::serialize();
        SerialString* v_ss = v_->serialized()->clone();

        size_t size = g_ss->size_ + v_ss->size_;
        char* arr = new char[size];
        memcpy(arr, g_ss->data_, g_ss->size_);
        memcpy(arr + g_ss->size_, v_ss->data_, v_ss->size_);

        delete(g_ss);
        delete(v_ss);
        
        SerialString* ss = new SerialString(arr, size);
        delete[](arr);

        return ss;
    }

    static Put* deserialize(SerialString* string) {
        Get* g = Get::deserialize(string);
        SerialString* g_ss = g->serialize();

        SerialString* val_substr = new SerialString(string->data_ + g_ss->size_, string->size_ - g_ss->size_);
        Value* v = new Value(val_substr);
        delete(val_substr);
        delete(g_ss);

        Put* p = new Put(*g, *g->k_, *v);

        delete(g);
        delete(v);
        return p;
    }

    bool equals(Object* other) {
        if(!Get::equals(other)) return false;
        Put* cast = dynamic_cast<Put *>(other);
        if(cast == nullptr) return false;
        return v_->equals(cast->v_);
    }
};

class Status : public Message {
public:
    Value* v_; // owned

    Status(Message& m, Value& v) : Message(m) {
        v_ = v.clone();
    }

    Status(size_t target, Value* v) : Message(MsgType::Status, target) {
        v_ = v->clone();
    }

    ~Status() {
        delete(v_);
    }

    SerialString* serialize() {
        SerialString* m_ss = Message::serialize();
        SerialString* v_ss = v_->serialized()->clone();

        size_t size = m_ss->size_ + v_ss->size_;
        char* arr = new char[size];
        memcpy(arr, m_ss->data_, m_ss->size_);
        memcpy(arr + m_ss->size_, v_ss->data_, v_ss->size_);

        delete(m_ss);
        delete(v_ss);

        SerialString* ss = new SerialString(arr, size);
        delete[](arr);

        return ss;
    }

    static Status* deserialize(SerialString* string) {
        Message* m = Message::deserialize_(string);

        SerialString* val_substr = new SerialString(string->data_ + (3 * sizeof(size_t)), string->size_ - (3 * sizeof(size_t)));
        Value* v = new Value(val_substr);
        delete(val_substr);

        Status* s = new Status(*m, *v);
        
        delete(m);
        delete(v);
        return s;
    }

    bool equals(Object* other) {
        if(!Message::equals(other)) return false;
        Status* cast = dynamic_cast<Status *>(other);
        if(cast == nullptr) return false;
        return v_->equals(cast->v_);
    }
};

class Directory : public Message {
public:
    size_t num_nodes_;
    size_t* ports_; // owned
    String** addresses_; // owned

    Directory(Message& m, size_t num_nodes, size_t* ports, String** addresses) : Message(m) {
        num_nodes_ = num_nodes;
        ports_ = ports;
        addresses_ = addresses;
    }

    // takes ownersip of ports and addresses
    Directory(size_t num_nodes, size_t* ports, String** addresses) : Message(MsgType::Directory, 0) {
        num_nodes_ = num_nodes;
        ports_ = ports;
        addresses_ = addresses;
    }

    SerialString* serialize() {
        SerialString* m_ss = Message::serialize();

        // serialize addresses
        SerialString** addresses_ss = new SerialString*[num_nodes_];
        size_t addresses_size = 0;
        for (size_t i = 0; i < num_nodes_; i++)
        {
            size_t s = addresses_[i]->size();
            char* str = new char[sizeof(size_t) + s];
            memcpy(str, &s, sizeof(size_t));
            memcpy(str + sizeof(size_t), addresses_[i]->c_str(), s);

            addresses_ss[i] = new SerialString(str, s + sizeof(size_t));
            addresses_size += addresses_ss[i]->size_;
        }
        

        size_t size = m_ss->size_ + sizeof(size_t) + (num_nodes_ * sizeof(size_t)) + addresses_size;
        char* arr = new char[size];

        size_t pos = 0;
        memcpy(arr, m_ss->data_, m_ss->size_);
        pos += m_ss->size_;
        delete(m_ss);

        memcpy(arr + pos, &num_nodes_, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(arr + pos, ports_, num_nodes_ * sizeof(size_t));
        pos += num_nodes_ * sizeof(size_t);

        for (size_t i = 0; i < num_nodes_; i++)
        {
            SerialString* a_ss = addresses_ss[i];
            memcpy(arr + pos, a_ss->data_, a_ss->size_);
            pos += a_ss->size_;
            delete(a_ss);
        }
        delete[](addresses_ss);

        SerialString* ss = new SerialString(arr, pos);
        delete[](arr);
        
        return ss;
    }

    static Directory* deserialize(SerialString* string) {
        Message* m = Message::deserialize_(string);
        size_t nodes;

        size_t pos = 3 * sizeof(size_t);
        memcpy(&nodes, string->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        size_t* ports = new size_t[nodes];
        String** addresses = new String*[nodes];

        memcpy(ports, string->data_ + pos, nodes * sizeof(size_t));
        pos += nodes * sizeof(size_t);

        for (size_t i = 0; i < nodes; i++)
        {
            size_t sz;
            memcpy(&sz, string->data_ + pos, sizeof(size_t));
            pos += sizeof(size_t);

            char* arr = new char[sz + 1];
            arr[sz] = '\0';

            memcpy(arr, string->data_ + pos, sz);
            pos += sz;

            addresses[i] = new String(arr);
            delete[](arr);
        }
        
        Directory* d = new Directory(*m, nodes, ports, addresses);

        delete(m);
        return d;
    }

    bool equals(Object* other) {
        if(!Message::equals(other)) return false;
        Directory* cast = dynamic_cast<Directory *>(other);
        if(cast == nullptr) return false;
        if(num_nodes_ != cast->num_nodes_) return false;
        for (size_t i = 0; i < num_nodes_; i++)
        {
            if(ports_[i] != cast->ports_[i]) return false;
            if(!addresses_[i]->equals(cast->addresses_[i])) return false;
        }
        return true;
    }
};

static Message* msg_deserialize(SerialString* serial) {
    size_t type;
    memcpy(&type, serial->data_, sizeof(size_t));
    switch(static_cast<MsgType>(type)) {
        case MsgType::Register:
            return Register::deserialize(serial);
        case MsgType::Get:
            return Get::deserialize(serial);
        case MsgType::Put:
            return Put::deserialize(serial);
        case MsgType::Status:
            return Status::deserialize(serial);
        case MsgType::Directory:
            return Directory::deserialize(serial);
        case MsgType::Fail:
            return Fail::deserialize(serial);
        default:
            assert(false);
            return nullptr;
    }
}

static Lock LOG_LOCK;

class Logger : public Object {
public:
    static void log(char* msg) {
        // Sys s;
        LOG_LOCK.lock();

        LOG_LOCK.unlock();
    }

    static void log_msg(Message* m) {
        Sys s;
        s.p(m->sender_).p(" to ").p(m->target_).p(" of type ");
        switch(m->type_) {
            case MsgType::Register:
                s.p("Register");
                break;
            case MsgType::Get:
                s.p("Get for key ").p(dynamic_cast<Get *>(m)->k_->name_)
                 .p(" in node ").p(dynamic_cast<Get *>(m)->k_->idx_);
                break;
            case MsgType::Put:
                s.p("Put for key ").p(dynamic_cast<Put *>(m)->k_->name_)
                 .p(" in node ").p(dynamic_cast<Put *>(m)->k_->idx_);
                break;
            case MsgType::Status:
                s.p("Status");
                break;
            case MsgType::Directory:
                s.p("Directory");
                break;
            case MsgType::Fail:
                s.p("Fail for key ").p(dynamic_cast<Fail *>(m)->k_->name_)
                 .p(" in node ").p(dynamic_cast<Fail *>(m)->k_->idx_);
                break;
            default:
                assert(false);
                return;
        }
        s.p(" and size ").pln(m->serialize()->size_);
    }

    static void log_send(Message* m) {
        Sys s;
        LOG_LOCK.lock();
        s.p("Message sent from ");
        log_msg(m);
        LOG_LOCK.unlock();
    } 

    static void log_receive(Message* m) {
        Sys s;
        LOG_LOCK.lock();
        s.p("Message received from ");
        log_msg(m);
        LOG_LOCK.unlock();
    }
};