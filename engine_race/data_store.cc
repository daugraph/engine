// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include <cassert>
#include <thread>

#include "util.h"
#include "data_store.h"
#include "data_store_imp.h"


namespace polar_race {

    DataStore::DataStore(const std::string& path, const int& max_open)
        : dir_(path), max_open_(max_open) {
        }

    void DataStore::Recover_cache(int index, DoorPlate *door)
    {
        int div=datafile_num/64;
        for(int i=0;i<div;i++)
            data_[div*index+i]->RecoverFile();
    }
    RetCode DataStore::Init(DoorPlate *door) {
        // printf("max_open_: %d\n", max_open_);
        datafile_num=FILE_NUM/DIV_PART;
        data_.reserve(datafile_num);
        for(int i = 0; i < datafile_num; i++) {
                data_.emplace_back(new DataStoreImp(dir_, i));
                data_[i]->Init(door);
        } 
        if(door->totalsum>0)
        {
            std::vector<std::thread>thread_vec1;
            for(int i = 0; i < 64; i++) {
                thread_vec1.emplace_back(std::thread(&DataStore::Recover_cache,this,i,door));
            }
            // 等待线程终止
            for(auto& th : thread_vec1) th.join();
        }
        
        // thread_vec1.clear();
        // std::map<int ,int>fdcount;
        // for(int i=0;i<max_open_;i++)
        // {
        //     if(fdcount.count(data_[i]->getFd())>0)
        //     {
        //         printf("dump fd: %d\n file1:%d, file2:%d\n",data_[i]->getFd(),i,fdcount[data_[i]->getFd()]);
        //         break;
        //     }
        //     fdcount[i]=data_[i]->getFd();
        // }
            

     
        return kSucc;
    }

    RetCode DataStore::Read(const uint64_t& index, char *value, const uint64_t& offset) {
        
        int file_no=index%datafile_num;
        int ind=index/datafile_num;
        RetCode ret = data_[file_no]->Read(offset, value,ind);
        return ret;
    }

    RetCode DataStore::Append(const uint64_t& index, const char *value, uint64_t* offset) {
        // RetCode ret = data_[index]->Append(value, offset);
        int file_no=index%datafile_num;
        int ind=index/datafile_num;
        // printf("write index: %llu, file_no:%d, ind:%d\n",index,file_no,ind);
        RetCode ret = data_[file_no]->AppendCache(value, offset,ind);

        // assert(ret == kSucc);
        return ret;
    }

    std::vector<DataStoreImp*>& DataStore::Data(){
        return data_;
     }


    DataStore::~DataStore() {
        for(auto& dsi : data_) delete dsi;
    }

}  // namespace polar_race