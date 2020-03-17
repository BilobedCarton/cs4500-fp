//lang:CwC

#pragma once

#include <netinet/in.h>
#include <string.h>

#include "object.h"
#include "string.h"
#include "serial.h"

enum class MsgKind { Ack = 0, Nack, Put, Reply, Get, WaitAndGet, Status, Kill, Register, Directory, Disconnect };

static size_t _ID_COUNTER = 0;

char* sockaddr_in_to_str(struct sockaddr_in in) {
  StrBuff buf;
  buf.c(buf.short_to_str(in.sin_family));
  buf.addc(',');
  buf.c(buf.short_to_str(in.sin_port));
  buf.addc(',');
  buf.c(buf.long_to_str(in.sin_addr.s_addr));
  buf.addc(',');
  buf.c(in.sin_zero);

  String* str = buf.get();
  char* serialized = new char[str->size_];
  strncpy(serialized, str->c_str(), str->size_);
  delete(str);
  return serialized;
}

class Message : public SerializableObject {
public:
  MsgKind kind_;  // the message kind
  size_t sender_; // the index of the sender node
  size_t target_; // the index of the receiver node
  size_t id_;     // an id t unique within the node

  Message() { }

  Message(MsgKind kind, size_t sender, int target) {
    id_ = _ID_COUNTER++;
    kind_ = kind;
    sender_ = sender;
    target_ = target;
  }

  Message(int kind, size_t sender, int target) : Message(static_cast<MsgKind>(kind), sender, target) { }

  ~Message() {}

  virtual char* serialize() { 
    StrBuff buf;
    buf.c("MG|");

    buf.c(size_t_to_str(static_cast<size_t>(kind_)));
    buf.addc('|');
    buf.c(size_t_to_str(sender_));
    buf.addc('|');
    buf.c(size_t_to_str(target_));
    buf.addc('|');
    buf.c(size_t_to_str(id_));

    String* str = buf.get();
    char* serialized = new char[str->size_];
    strncpy(serialized, str->c_str(), str->size_);
    delete(str);
    return serialized;
  }

  static Message* deserialize(char* serialized) {
    Message* m = new Message();

    char* buf = strtok(serialized+3, "|");
    m->kind_ = static_cast<MsgKind>(atoi(buf));
    //delete[](buf);
    buf = strtok(NULL, "|");
    m->sender_ = atoi(buf);
    //delete[](buf);
    buf = strtok(NULL, "|");
    m->target_ = atoi(buf);
    //delete[](buf);
    buf = strtok(NULL, "|");
    m->id_ = atoi(buf);
    //delete[](buf);

    return m;
  }
};

class Ack : public Message { }; 

class Status : public Message {
public:
  String* msg_; // owned

  Status() : Message() { }

  Status(String* msg, size_t sender, size_t target) : Message(6, sender, target) {
    msg_ = new String(*msg);
  }

  ~Status() { delete(msg_); }

  char* serialize() { 
    char* serialized_partial = Message::serialize();
    StrBuff buf;
    buf.c(serialized_partial);
    delete[](serialized_partial);
    buf.addc('|');
    buf.c(msg_->c_str());

    String* str = buf.get();
    char* serialized = new char[str->size_ + 1];
    strncpy(serialized, str->c_str(), str->size_);
    serialized[str->size_] = '\0';
    delete(str);
    return serialized;
  }

  static Status* deserialize(char* serialized) {
    Status* s = new Status();

    Message* m = dynamic_cast<Message *>(Message::deserialize(serialized));
    s->kind_ = m->kind_;
    s->sender_ = m->sender_;
    s->target_ = m->target_;
    s->id_ = m->id_;

    char* buf = strtok(NULL, "|");
    s->msg_ = new String(buf);

    delete(m);
    return s;
  }
};

class Register : public Message {
public:
  struct sockaddr_in client;
  size_t port;

  Register() : Message() { }

  Register(struct sockaddr_in c, size_t p, size_t sender, size_t target) : Message(8, sender, target) {
    client.sin_family = c.sin_family;
    client.sin_port = c.sin_port;
    client.sin_addr.s_addr = c.sin_addr.s_addr;
    port = p;
  }

  char* serialize() { 
    char* serialized_partial = Message::serialize();
    StrBuff buf;
    buf.c(serialized_partial);
    delete[](serialized_partial);
    buf.addc('|');
    buf.c(sockaddr_in_to_str(client));
    buf.addc('|');
    buf.c(size_t_to_str(port));

    String* str = buf.get();
    char* serialized = new char[str->size_];
    strncpy(serialized, str->c_str(), str->size_);
    delete(str);
    return serialized;
  }

  static Register* deserialize(char* serialized) {
    Register* s = new Register();

    Message* m = dynamic_cast<Message *>(Message::deserialize(serialized));
    s->kind_ = m->kind_;
    s->sender_ = m->sender_;
    s->target_ = m->target_;
    s->id_ = m->id_;

    char* client_buf = strtok(NULL, "|"); // client

    char* buf = strtok(NULL, "|");
    s->port = atoi(buf);
    //delete[](buf);

    // parse client buf
    buf = strtok(client_buf, ",");
    s->client.sin_family = (short)atoi(buf);
    //delete[](buf);
    buf = strtok(NULL, ",");
    s->client.sin_port = (unsigned short)strtol(buf, NULL, 10);
    //delete[](buf);
    buf = strtok(NULL, ",");
    s->client.sin_addr.s_addr = strtol(buf, NULL, 10);
    //delete[](buf);
    buf = strtok(NULL, ",");
    if(buf != nullptr) memcpy((char*)s->client.sin_zero, buf, strlen(buf));
    //delete[](buf);

    delete(m);
    return s;
  }
};

class Directory : public Message {
public:
  size_t clients;
  size_t * ids;
  size_t * ports;  // owned
  StringArray* addresses;  // owned; strings owned

  Directory() : Message() { }

  Directory(size_t c, size_t* id, size_t* p, StringArray* a, size_t sender, size_t target) : Message(9, sender, target) {
    clients = c;
    ids = new size_t[clients];
    ports = new size_t[clients];
    addresses = new StringArray(clients);
    for (int i = 0; i < clients; ++i)
    {
      ids[i] = id[i];
      ports[i] = p[i];
      addresses->append(a->get(i));
    }
  }

  ~Directory() {
    delete[](ports);
    delete(addresses);
  }

  char* serialize() { 
    char* serialized_partial = Message::serialize();
    StrBuff buf;
    buf.c(serialized_partial);
    delete[](serialized_partial);
    buf.addc('|');
    buf.c(size_t_to_str(clients));
    buf.addc('|');
    for (int i = 0; i < clients; ++i)
    {
      buf.c(size_t_to_str(ids[i]));
      if(i != clients - 1) buf.addc(',');
    }
    buf.addc('|');
    for (int i = 0; i < clients; ++i)
    {
      buf.c(size_t_to_str(ports[i]));
      if(i != clients - 1) buf.addc(',');
    }
    buf.addc('|');
    buf.c(addresses->serialize());

    String* str = buf.get();
    char* serialized = new char[str->size_];
    strncpy(serialized, str->c_str(), str->size_);
    delete(str);
    return serialized;
  }

  static Directory* deserialize(char* serialized) {
    Directory* s = new Directory();

    Message* m = dynamic_cast<Message *>(Message::deserialize(serialized));
    s->kind_ = m->kind_;
    s->sender_ = m->sender_;
    s->target_ = m->target_;
    s->id_ = m->id_;

    char* buf = strtok(NULL, "|");
    s->clients = atoi(buf);

    // set aside for later
    char* ids_buf = strtok(NULL, "|");
    char* ports_buf = strtok(NULL, "|");

    buf = strtok(NULL, "|");
    s->addresses = dynamic_cast<StringArray *>(StringArray::deserialize(buf));

    // ids
    s->ids = new size_t[s->clients];
    buf = strtok(ids_buf, ",");
    s->ids[0] = atoi(buf);
    for (int i = 1; i < s->clients; ++i)
    {
      buf = strtok(NULL, ",");
      s->ids[i] = atoi(buf);
    }

    // ports
    s->ports = new size_t[s->clients];
    buf = strtok(ports_buf, ",");
    s->ports[0] = atoi(buf);
    for (int i = 1; i < s->clients; ++i)
    {
      buf = strtok(NULL, ",");
      s->ports[i] = atoi(buf);
    }

    delete(m);
    return s;
  }
};