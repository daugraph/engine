// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_DOOR_PLATE_IMP_H_
#define ENGINE_RACE_DOOR_PLATE_IMP_H_

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>

#include "util.h"
#include "door_plate.h"
#include "include/engine.h"
#include "para.h"


namespace polar_race {
   
    struct Item {
        Item(const uint64_t& key_, const  unsigned int& offset_)
            : key(key_), offset(offset_) {}
            
        uint64_t key;
        unsigned int offset;

        bool operator < (const Item &rhs) const {
            if(key< rhs.key)
                return true;
            if(key == rhs.key && offset > rhs.offset)
                return true;
            return false;
        }

        bool operator == (const Item &rhs) const {
            return key == rhs.key;
        }
    };

   



    class DoorPlateImp  {
        public:
            DoorPlateImp();

            DoorPlateImp(const std::string& path, const int& file_no);

            ~DoorPlateImp();

            RetCode Init(uint64_t Map_size);

            // 多线程调用
            RetCode operator() ();
            
            RetCode Append(const char* key, const uint64_t& offset);

            RetCode Find(const uint64_t& key, uint64_t* offset);

            std::vector<Item>& Data();

            RetCode RecoveryIndex();
            RetCode get_unique();
            
            uint64_t size_;  
            uint64_t unique_count;

            char *index_ptr;


        private:
            // 文件的路径,需要由调用者提供
            std::string dir_;

            // 文件描述符
            int fd_;

            // 文件编号
            int file_no_;

            // 指向一个排好序的 Item 数组
            std::vector<Item> items_;
            // Item* items_;

            // 记录当前索引文件有多少记录数
             
            uint64_t mapsize_;          

            char write_buf[12];



            // 从文件中恢复索引

    };

}  // namespace polar_race

#endif 