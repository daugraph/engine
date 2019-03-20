// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <thread>
#include <unistd.h>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <mutex>
#include <chrono>
#include <thread>
#include "util.h"
#include "engine_race.h"
#include "para.h"


namespace polar_race {

    static char * getBuffer(){
        // printf("page size=%d\n", getpagesize());
        char*temp=(char*)malloc(BLOCK_SIZE);
        if(posix_memalign ((void**)&temp, getpagesize(), BLOCK_SIZE))
            return NULL;
        return temp;
    }
    static thread_local std::unique_ptr<char> readBuffer(getBuffer());

    inline int getIndex(const char *k) {
            return ((u_int16_t) ((u_int8_t) k[0]) << 3) | ((u_int8_t) k[1] >> 5);
    }

  


    RetCode Engine::Open(const std::string& name, Engine** eptr) { 
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {}

    // void startBufferPrepared(NaiveRange* rangetask,DoorPlate* door,DataStore* data)
    // {
    //     rangetask->writeByBuffer(door,data);
    // }
    // pthread_spin_lock (pthread_spinlock_t *lock);
    // pthread_spin_trylock (pthread_spinlock_t *lock);
    // pthread_spin_unlock (pthread_spinlock_t *lock);
    RetCode loading(NaiveRange* range, DataStore* data,int index)
    {
        auto datafile=data->Data();
        int fd=datafile[index]->getFd();
        printf("fd:%d index:%d\n",fd,index);
        range->loadFile(fd,index);
        return kSucc;
    }

    RetCode EngineRace::Open(const std::string& name, Engine** eptr) {

        auto t1 = std::chrono::high_resolution_clock::now();

        printf("\n++++++++++++++++ open engine ++++++++++++++++\n");   
        printf("engine path: %s\n", name.c_str());
        
        if (!FileExists(name) && 0 != mkdir(name.c_str(), 0755)) { return kIOError;}
        *eptr = NULL;
        EngineRace *engine = new EngineRace(name);
        engine->thread_index=0;
        engine->starttime=t1;

        DataStore *data_ = new DataStore(name, FILE_NUM);
        engine->data_ = data_;

        // 创建 door_place 对象
        // std::vector<std::thread> thread_vec;
        // for(int i = 0; i < BLOCK_NUM; i++) {
        //     thread_vec.emplace_back(std::thread(loading,engine->rangetask,data_,i));
        // }
        // for(auto& th : thread_vec) th.detach();

        
        DoorPlate *door_ = new DoorPlate(name, FILE_NUM);
        
        door_->Init();
        auto t2 = std::chrono::high_resolution_clock::now();
        engine->door_ = door_;
        int sum=0;

        auto indexfile=engine->door_->Data();
        for(auto& item:indexfile)
        {
            sum+=item->Data().size();
        }
        engine->totalsum=sum;
        engine->door_->totalsum=sum;
        printf("Total sum:%d\n",sum);
        if(sum>10000000)
        {
             engine->rangetask = new NaiveRange(name,FILE_NUM);
             engine->rangetask->Init();
        }
        std::cout << "recover index takes: "
                  << std::chrono::duration<double, std::milli>(t2 - t1).count()
                  << " milliseconds" << std::endl;
        // 创建 data_store 对象
        data_->Init(door_);
       
        
        
   
        auto t3 = std::chrono::high_resolution_clock::now();

        std::cout << "open engine takes: "
                  << std::chrono::duration<double, std::milli>(t3 - t1).count()
                  << " milliseconds" << std::endl;
        *eptr = engine;
        return kSucc;

    }


    EngineRace::~EngineRace() {
        auto end1=std::chrono::high_resolution_clock::now();
        // delete door_;
        // delete data_;
        // delete rangetask;
        printf("++++++++++++++++ close engine ++++++++++++++++\n\n");
        auto end=std::chrono::high_resolution_clock::now();
        std::cout << "close  takes: "
                  << std::chrono::duration<double, std::milli>(end - end1).count()
                  << " milliseconds" << std::endl;
        std::cout << "total task takes: "
                  << std::chrono::duration<double, std::milli>(end - starttime).count()
                  << " milliseconds" << std::endl;
    }

    RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {

        // printf("write key: %s\n", key.data());
        // 获得key所在的文件编号
        // uint64_t index = getIndex(key.data());
        uint64_t offset = 0;
        uint64_t num_key = __builtin_bswap64(*(uint64_t*)key.data());
        uint64_t index = num_key>>MOVEPOS;
      
        // 先写数据
        
        // printf("WRITE: num_key: %lu, index: %lu, offset: %lu\n", num_key, index, offset);
        data_->Append(index, value.data(), &offset);
        // data_->AppendCache(index, value.data(), &offset);
        
        // 再写索引
        door_->Append(index, key.data(), offset);

        return kSucc;
    }

    RetCode EngineRace::Read(const PolarString& key, std::string* value) {
        // printf("enter into read!\n");

        // 获得key所在的文件编号
        // uint64_t num_key = ByteToUnsignedInt64(key.data());
        uint64_t num_key = __builtin_bswap64(*(uint64_t*)key.data());
        uint64_t index = num_key>>MOVEPOS;
        // 获得偏移值
        uint64_t offset = 0;
        if(kNotFound == door_->Find(index, num_key, &offset)) {
            return kNotFound;
        }
        // 再根据偏移值读取数据
        auto read_buf=readBuffer.get();
        data_->Read(index,read_buf, offset);
        value->assign(read_buf, BLOCK_SIZE);
     

        return kSucc;
    }

    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper, Visitor &visitor) {
        // printf("lower: %s, upper: %s\n", lower.data(), upper.data());

        // assert(lower.data() == "" && upper.data() == "");



        thread_lock.lock();
        int cur_index=thread_index;
        thread_index++;
        thread_lock.unlock();
        
        if(totalsum>10000000)
        // if(true)
        {
            if(cur_index==0)
            {
                    // printf("open:  door size:%lu\n",engine->door_->Data().size());
                    std::vector<std::thread>writeTask;
                    for(int i=0;i<THREAD_NUM;i++)
                         writeTask.emplace_back(std::thread(&NaiveRange::competeBlock,rangetask,door_,data_));
                        // writeTask.emplace_back(std::thread(&NaiveRange::writeBlock,rangetask,i,door_,data_));
                    for(auto& th: writeTask)
                        th.detach();
            }
            this->rangetask->rangeByBuffer(visitor,door_,data_);
        }
       else{
            // char *buf = (char *) malloc (BLOCK_SIZE);
            char str_key[8];
            auto buf= readBuffer.get();
            // uint64_t cnt = 0;
            auto door = door_->Data();
            for(unsigned int index = 0; index < door.size(); index++) {
            // 排序去重过的items
                auto items = door[index]->Data();
                for(unsigned int ind=0;ind<door[index]->unique_count;ind++) {
                    uint64_t key = items[ind].key;
                    uint64_t offset = items[ind].offset;
                    data_->Read(key >> MOVEPOS, buf, offset);
                    for(int i = 0; i < 8; i++)
                        str_key[i] = (char)(key >> (56 - 8 * i));
                    PolarString key_(str_key, 8);
                    PolarString value_(buf, BLOCK_SIZE);
                    visitor.Visit(key_, value_);
                    // cnt++;
                }
            }
        } 
   


        return kSucc;
    }

}  // namespace polar_race