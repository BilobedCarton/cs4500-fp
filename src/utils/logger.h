#pragma once

#include "object.h"
#include "thread.h"
#include "../store/message.h"

static Lock LOG_LOCK;
static bool SUPPRESS_LOGGING = false;

class Logger : public Object {
public:
    static void log(char* msg) {
        if(SUPPRESS_LOGGING) return;
        Sys s;
        LOG_LOCK.lock();
        s.pln(msg);
        LOG_LOCK.unlock();
    }

    static void log_register(size_t idx, char* key) {
        if(SUPPRESS_LOGGING) return;
        Sys s;
        LOG_LOCK.lock();
        s.p("Registered node ").p(idx).p(" as ").pln(key);
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
        if(SUPPRESS_LOGGING) return;
        Sys s;
        LOG_LOCK.lock();
        s.p("Message sent from ");
        log_msg(m);
        LOG_LOCK.unlock();
    } 

    static void log_receive(Message* m) {
        if(SUPPRESS_LOGGING) return;
        Sys s;
        LOG_LOCK.lock();
        s.p("Message received from ");
        log_msg(m);
        LOG_LOCK.unlock();
    }
};