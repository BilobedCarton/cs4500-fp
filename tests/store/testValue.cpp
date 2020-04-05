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

    SerialString* serialize() {
        size_t m_len = strlen(message);
        char* arr = new char[sizeof(size_t) + sizeof(double) + m_len];
        size_t pos = 0;

        memcpy(arr, &number, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(arr + pos, &percentage, sizeof(double));
        pos += sizeof(double);

        memcpy(arr + pos, message, m_len);
        pos += m_len;

        SerialString* ss = new SerialString(arr, pos);
        delete[](arr);

        return ss;
    }

    static TestSO* deserialize(SerialString* serialized) {
        size_t n;
        double p;

        size_t pos = 0;

        memcpy(&n, serialized->data_ + pos, sizeof(size_t));
        pos += sizeof(size_t);

        memcpy(&p, serialized->data_ + pos, sizeof(double));
        pos += sizeof(double);

        char* m = new char[serialized->size_ - pos];
        memcpy(m, serialized->data_ + pos, serialized->size_ - pos);

        TestSO* so = new TestSO(n, p, m);
        delete[](m);
        return so;
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
        assert(strcmp(v_reg->serialized()->data_, so1->serialize()->data_) == 0);
        OK("Value::Value(so) -- passed.");

        assert(v_cache->serialized_ == nullptr);
        assert(strcmp(v_cache->file_, file_) == 0);
        assert(v_cache->position_ == 20);
        assert(v_cache->size_ == so2->serialize()->size_);

        FILE* f = fopen(file_, "r");
        fseek(f, v_cache->position_, SEEK_SET);
        char* temp = new char[v_cache->size_ + 1];
        fread(temp, sizeof(char), v_cache->size_, f);
        temp[v_cache->size_] = '\0';
        fclose(f);

        assert(strcmp(temp, so2->serialize()->data_) == 0);

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

        assert(strcmp(reg_clone->serialized_->data_, v_reg->serialized_->data_) == 0);
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
        assert(strlen(v_cache->serialized_->data_) == v_cache->size_ / sizeof(char));

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
        assert(strcmp(v_reg->serialized()->data_, so1->serialize()->data_) == 0);
        assert(strcmp(v_cache->serialized()->data_, so2->serialize()->data_) == 0);
        assert(strcmp(v_reg->serialized()->data_, so2->serialize()->data_) != 0);
        assert(strcmp(v_cache->serialized()->data_, so1->serialize()->data_) != 0);

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