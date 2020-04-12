#pragma once

#include <stddef.h>
#include <assert.h>
#include <string.h>

#include "object.h"

#define NUM_NODES_FLAG "-nn"
#define PORT_FLAG "-p"
#define IDX_FLAG "-idx"
#define SERVER_ADR_FLAG "-sa"
#define SERVER_PORT_FLAG "-sp"

class Args : public Object {
public:
    size_t num_nodes = 0;
    size_t port = 0;
    int idx = -1;
    char* server_adr = nullptr;
    size_t server_port = 0;
    
    Args(int argc, char** argv) {
        for (size_t i = 1; i < argc; i+=2)
        {
            read_flag(argv[i], argv[i + 1]);
        }
        verify();
    }

    void read_flag(char* flag, char* value) {
        if(strcmp(flag, NUM_NODES_FLAG) == 0) {
            num_nodes = atol(value);
            assert(strcmp(value, to_str<size_t>(num_nodes)) == 0);
        } else if(strcmp(flag, PORT_FLAG) == 0) {
            port = atol(value);
            assert(strcmp(value, to_str<size_t>(port)) == 0);
        } else if(strcmp(flag, IDX_FLAG) == 0) {
            idx = atoi(value);
            assert(strcmp(value, to_str<int>(idx)) == 0);
        } else if(strcmp(flag, SERVER_ADR_FLAG) == 0) {
            server_adr = duplicate(value);
        } else if(strcmp(flag, SERVER_PORT_FLAG) == 0) {
            server_port = atol(value);
            assert(strcmp(value, to_str<size_t>(server_port)) == 0);
        } else {
            assert(false);
        }
    }

    void verify() {
        assert(num_nodes > 0);
        assert(port != 0);
        assert(idx >= 0);
        if(idx != 0) {
            assert(server_adr != nullptr);
            assert(server_port != 0);
        }
    }
};

static Args* args = nullptr;

static void CreateArgs(int argc, char** argv) {
    args = new Args(argc, argv);
}