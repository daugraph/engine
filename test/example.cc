#include <bits/stdc++.h>
#include <assert.h>
#include <stdio.h>
#include <string>
#include "include/engine.h"

#include <thread>
#include <mutex>
#include <random>
#include <iostream>
#include <algorithm>

#include <set>
    
using namespace std;

typedef unsigned int uint32;
typedef unsigned long long uint64;
typedef unsigned char byte;

static const char kEnginePath[] = "/home/clf/test_engine";

using namespace polar_race;

int main(int argc, char const *argv[])
{
    Engine *engine = NULL;
    RetCode ret = Engine::Open(kEnginePath, &engine);
    assert (ret == kSucc);

    // engine->Write("aaaabbbb", "34545454554");
    // engine->Write("bbbbcccc", "123123123");
    // engine->Write("aaaabbbb", "34545454554");
    // engine->Write("bbbbcccc", "123123123");
    // engine->Write("aaaabbbb", "xxxxxxx");
    // engine->Write("bbbbcccc", "123123123");
    // engine->Write("aaaabbbb", "succeess");
    // engine->Write("bbbbcccc", "ddddddddd");

    std::string v;
    engine->Read("aaaabbbb", &v);
     std::cout << v << std::endl;
    engine->Read("bbbbcccc", &v);
    std::cout << v << std::endl;

    return 0;
}
wget http://polardbrace.oss-cn-shanghai.aliyuncs.com/log_197704.tar
http://polardbrace.oss-cn-shanghai.aliyuncs.com/log_197704.tar
tar xvf log_197704.tar

g++ -std=c++11 -o test -g -I.. test.cc -L../lib -lengine -lpthread -lrt -lz