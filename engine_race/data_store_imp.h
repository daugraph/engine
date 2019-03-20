// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DATA_STORE_IMP_H_
#define ENGINE_RACE_DATA_STORE_IMP_H_
#include <stdint.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <aio.h>
#include <mutex>
#include "door_plate.h"
#include "para.h"
#include "include/engine.h"


namespace polar_race {

    class DataStoreImp  {
        public:
            explicit DataStoreImp(const std::string& path, const int& file_no);

            ~DataStoreImp();

            RetCode Init(DoorPlate *door);
            
            RetCode Append(const char* data, uint64_t* offset);

            // RetCode AppendCache(const char* data, uint64_t* offset);
            RetCode AppendCache(const char* data, uint64_t* offset,int index);


            RetCode RecoverFile();

            // RetCode Read(const uint64_t& offset, char* buf);

            RetCode Read(const uint64_t& offset, char* buf,int index);
            uint64_t getSize(const int &index);
            int getFd();


        private:
            // 文件的路径,需要由调用者提供
            std::string dir_;

            // 文件描述符
            int fd_;
            int fd1;

            // 文件编号
            int file_no_;

            // 记录当前数据文件有记录数
            uint64_t size_;

            // 写入文件需要加锁
            // std::mutex *mtx_;

            // char* write_buf;

            // char* cache_buf;

            // int write_pos;
            // int writecount;
            uint64_t file_interval;
            uint64_t file_size[DIV_PART];
            int writecount[DIV_PART];
            int write_pos[DIV_PART];
            char* cache_buf[DIV_PART];
            char* write_buf[DIV_PART];
            std::mutex mtx_[DIV_PART];

    };

}  // namespace polar_race

#endif // 结束