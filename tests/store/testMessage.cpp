#include <assert.h>

#include "../test.h"
#include "../../src/store/message.h"

class TestMessage : public Test {
public:
    String* ip1 = new String("111.111.111.111");
    Register* r = new Register(*ip1, 1021);
    Key* k1 = new Key("test", 0);
    Key* k2 = new Key("testo", 0);
    Get* g = new Get(k1);
    SerialString* ss = new SerialString("teststr", 7);
    Value* v = new Value(ss);
    Put* p = new Put(k2, v);
    Status* s = new Status(1, v);
    size_t* ports = new size_t[2] { 1024, 1001 };
    String* ip2 = new String("101.101.010.010");
    String* ip3 = new String("101.141.765.010");
    String** addresses = new String*[2] { ip2, ip3 };
    Directory* d = new Directory(2, ports, addresses);

    ~TestMessage() {
        delete(ip1);
        delete(r);
        delete(k1);
        delete(k2);
        delete(g);
        delete(ss);
        delete(v);
        delete(p);
        delete(s);
        delete[](ports);
        delete(ip2);
        delete(ip3);
        delete[](addresses);
        delete(d);
    }

    // only really care about serialization and deserialization

    bool testRegister() {
        Register* clone = Register::deserialize(r->serialize());
        assert(clone->equals(r));
        OK("Message::Register tests - passed.");
        return true;   
    }

    bool testGet() {
        Get* clone = Get::deserialize(g->serialize());
        assert(clone->equals(g));
        OK("Message::Get tests - passed.");
        return true;   
    }

    bool testPut() {
        Put* clone = Put::deserialize(p->serialize());
        assert(clone->equals(p));
        OK("Message::Put tests - passed.");
        return true;   
    }

    bool testStatus() {
        Status* clone = Status::deserialize(s->serialize());
        assert(clone->equals(s));
        OK("Message::Status tests - passed.");
        return true;   
    }

    bool testDirectory() {
        Directory* clone = Directory::deserialize(d->serialize());
        assert(clone->equals(d));
        OK("Message::Directory tests - passed.");
        return true;   
    }

    bool testMsgDeserialize() {
        Message** msgs = new Message*[5];
        msgs[0] = msg_deserialize(r->serialize());
        msgs[1] = msg_deserialize(g->serialize());
        msgs[2] = msg_deserialize(p->serialize());
        msgs[3] = msg_deserialize(s->serialize());
        msgs[4] = msg_deserialize(d->serialize());
        assert(msgs[0]->equals(r));
        assert(msgs[1]->equals(g));
        assert(msgs[2]->equals(p));
        assert(msgs[3]->equals(s));
        assert(msgs[4]->equals(d));
        for (size_t i = 0; i < 5; i++)
        {
            delete(msgs[i]);
        }
        delete[](msgs);
        OK("msg_deserialize(ss) -- passed.");
        return true;
    }

    bool run() {
        return testRegister()
            && testGet()
            && testPut()
            && testStatus()
            && testDirectory()
            && testMsgDeserialize();
    }
};

int main() {
    TestMessage test;
    test.testSuccess();
}