#include <assert.h>
#include <string.h>

#include "../../src/store/value.h"
#include "../../src/utils/serial.h"
#include "../../src/utils/string.h"
#include "../test.h"

class TestSO : public SerializableObject {
public:
    size_t number;
    double percentage;
    char* message;

    TestSO(size_t n, double p, const char* m) {
        number = n;
        percentage = p;
        message = duplicate(m);
    }

    ~TestSO() {
        delete[](message);
    }

    char* serialize() {
        StrBuff buf;
        buf.c("TE|");
        buf.c(to_str<size_t>(number));
        buf.addc('|');
        buf.c(to_str<double>(percentage));
        buf.addc('|');
        buf.c(message);
        String* s = buf.get();
        char* serialized = duplicate(s->c_str());
        delete(s);
        return serialized;
    }

    static TestSO* deserialize(char* serialized) {
        size_t n = atol(strtok(serialized+3, "|"));
        double p = atof(strtok(NULL, "|"));
        char* m = strtok(NULL, "|");
        return new TestSO(n, p, m);
    }
};

class TestValue : public Test {
public:
    TestSO* so1;
    TestSO* so2;
    char* file_;
    Value* v_reg;
    CachableValue* v_cache;

    TestValue(const char* file) {
        so1 = new TestSO(4, -100.9, "test message");
        so2 = new TestSO(10009872, -100.90000271, "other test message");
        file_ = duplicate(file);

        FILE* f = fopen(file_, "w");
        const char* msg = "Test File for Value\n";
        fwrite(msg, sizeof(char), strlen(msg), f);
        fclose(f);

        v_reg = new Value(so1);
        v_cache = new CachableValue(file_, so2);
    }

    ~TestValue() {
        delete(so1);
        delete(so2);
        remove(file_);
        delete[](file_);
        delete(v_reg);
        delete(v_cache);
    }

    bool testConstructor() {
        assert(strcmp(v_reg->serialized(), so1->serialize()) == 0);
        OK("Value::Value(so) -- passed.");

        assert(v_cache->serialized_ == nullptr);
        assert(strcmp(v_cache->file_, file_) == 0);
        assert(v_cache->position_ == 20);
        assert(v_cache->size_ == strlen(so2->serialize()));

        FILE* f = fopen(file_, "r");
        fseek(f, v_cache->position_, SEEK_SET);
        char* temp = new char[v_cache->size_ + 1];
        fread(temp, sizeof(char), v_cache->size_, f);
        temp[v_cache->size_] = '\0';
        fclose(f);

        assert(strcmp(temp, so2->serialize()) == 0);

        OK("CachableValue::CachableValue(...) -- passed.");
        return true;
    }

    bool testCachable() {
        assert(v_reg->cachable() == false);
        assert(v_cache->cachable() == true);

        OK("Value::Cachable() -- passed");
        return true;
    }

    bool testClone() {
        Value* reg_clone = v_reg->clone();
        CachableValue* cache_clone = dynamic_cast<CachableValue *>(v_cache->clone());

        assert(strcmp(reg_clone->serialized_, v_reg->serialized_) == 0);
        assert(strcmp(cache_clone->file_, v_cache->file_) == 0);
        assert(cache_clone->size_ == v_cache->size_);
        assert(cache_clone->position_ == v_cache->position_);

        delete(reg_clone);
        delete(cache_clone);
        OK("Value::Clone() -- passed");
        return true;
    }

    bool testCache() {
        v_cache->cache();
        assert(v_cache->serialized_ != nullptr);
        assert(strlen(v_cache->serialized_) == v_cache->size_ / sizeof(char));

        OK("CachableValue::Cache() -- passed");
        return true;
    }

    bool testUncache() {
        v_cache->uncache();
        assert(v_cache->serialized_ == nullptr);
        assert(v_cache->last_access_ == 0);

        OK("CachableValue::Uncache() -- passed");
        return true;
    }

    bool testSerialized() {
        assert(strcmp(v_reg->serialized(), so1->serialize()) == 0);
        assert(strcmp(v_cache->serialized(), so2->serialize()) == 0);
        assert(strcmp(v_reg->serialized(), so2->serialize()) != 0);
        assert(strcmp(v_cache->serialized(), so1->serialize()) != 0);

        OK("Value::Serialized() -- passed");
        return true;
    }

    bool run() {
        return testConstructor() 
            && testCachable() 
            && testClone() 
            && testCache() 
            && testUncache() 
            && testSerialized();
    }
};

int main() {
    TestValue test("testValue.eau2");
    test.testSuccess();
}