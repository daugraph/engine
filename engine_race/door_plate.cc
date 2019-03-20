// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include <thread>

#include "util.h"
#include "door_plate.h"


namespace polar_race {

    DoorPlate::DoorPlate(const std::string& path, const int max_open)
        : dir_(path), max_open_(max_open) {}
    
    void DoorPlate::index_recover(int index)
    {
        int div=max_open_/64;
        for(int i=0;i<div;i++)
            door_[div*index+i]->RecoveryIndex();
    }

    RetCode DoorPlate::Init() {
        door_.reserve(max_open_);
        int threadnum=64;
        uint64_t Mapsize=(64000000/FILE_NUM+5000)*8+CACHE_SIZE*BLOCK_SIZE;
        // for(int i=0;i<threadnum;i++)
        //     tempbuf[i]=(char*)malloc(Mapsize);

        for(int i = 0; i < max_open_; i++) {
            door_.emplace_back(new DoorPlateImp(dir_, i));
            door_[i]->Init(Mapsize);
        }
        std::vector<std::thread> thread_vec;
        for(int i = 0; i < threadnum; i++) {
            thread_vec.emplace_back(std::thread(&DoorPlate::index_recover,this,i));
        }

        // 等待线程终止
        for(auto& th : thread_vec) th.join();
        thread_vec.clear();

        // for(int i=0;i<max_open_;i++)
        //     door_[i]->get_unique();
     
        return kSucc;
    }

    RetCode DoorPlate::Find(const uint64_t& index, const uint64_t& key, uint64_t* offset) {
        RetCode ret = door_[index]->Find(key, offset);
        return ret;
    }

    RetCode DoorPlate::Append(const uint64_t& index, const char*key, const uint64_t& offset) {
        RetCode ret = door_[index]->Append(key, offset);
        return ret;
    }

    std::vector<DoorPlateImp*>& DoorPlate::Data() {
        return door_;
    }

    DoorPlate::~DoorPlate() {
        for(auto& dpi : door_) delete dpi;
    }

}  // namespace polar_race