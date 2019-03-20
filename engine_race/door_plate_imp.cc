// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include <algorithm>
#include <vector>
#include <unistd.h>

#include "util.h"
#include "door_plate.h"
#include "include/engine.h"
// 构造对象需要 path 和 file_no

namespace polar_race {

    static const char kMetaFileNamePrefix[] = "META_";

    static const uint64_t kKeySize = 8;

    static bool SearchCmp(const Item& a, const Item& b) { return a.key < b.key; }

    DoorPlateImp::DoorPlateImp(const std::string& path, const int& file_no): dir_(path), fd_(-1), file_no_(file_no) {}

    // 注意: 不能多线程同时open文件,这样会导致分配的文件描述符一样
    RetCode DoorPlateImp::Init(uint64_t Map_size) {

        // 判断文件是否存在,如果不存在创建文件
        std::string path = dir_ + "/" + kMetaFileNamePrefix + std::to_string(file_no_);

        int fd =  open(path.c_str(), O_RDWR | O_CREAT|O_NOATIME, 0655);
        
        items_.reserve(64000000/FILE_NUM+5000);
        // items_=(Item*)malloc(sizeof(Item)*Map_size/12);
        // size_=GetFileLength(path)/kKeySize;
        if (fd < 0) { return kIOError; }
        fd_ = fd;
        if(ftruncate(fd_, Map_size)<0)
            return kIOError;
        index_ptr=(char*)mmap(NULL, Map_size, PROT_READ | PROT_WRITE, MAP_SHARED,  fd, 0);
        if(index_ptr==NULL)
            printf("mmap failed!\n");
        mapsize_=Map_size;

        // RecoveryIndex();

        // 重建后打印出item里面的内容
        // for(const auto&it : items_) {
        //     printf("RECOVERY index: %d, key:%lu, offset:%lu\n", file_no_, it.key, it.offset);
        // }
        
        return kSucc;
    }

    // 返回值: 读到的字节数,若已到文件结尾则返回0,若出错返回-1
    // ssize_t pread(int filedes, void *buf, size_t nbytes, off_t offset);

    // 返回值: 若成功返回已写的字节数,若出错返回-1
    // ssize_t pwrite(int filedes, const void *buf, size_t nbytes, off_t offset);

    RetCode DoorPlateImp::RecoveryIndex() {
    
        //  if(size_>0)
        //     pread(fd_,buf,mapsize_*kKeySize,0);
        //  for(unsigned int i=0;i<mapsize_;i++)
        //  {
        //      items_.emplace_back(ByteToUnsignedInt64(buf+i*kKeySize),i);
        //  }
        // items_=reinterpret_cast<Item*>index_ptr;

        unsigned int cur_offset=0;
         while(1) {
            uint64_t intkey=ByteToUnsignedInt64(index_ptr+cur_offset*kKeySize);
            if(intkey == 0) {
                break;
            }
            items_.emplace_back(intkey, cur_offset);
            cur_offset++;
        }
        // 释放内存
        // free(buf);
        // 排序去重
        size_=cur_offset;
        std::sort(items_.begin(), items_.end());
        unique_count=unique(items_.begin(), items_.end())-items_.begin();
        // printf("recover finished, index no:%u!\n",file_no_);
        return kSucc;
    }
    RetCode DoorPlateImp::get_unique()
    {
        items_.erase(unique(items_.begin(), items_.end()), items_.end());
        items_.shrink_to_fit();
        return kSucc;
    }

    // 根据传进来的offset写入对应的索引文件,调用方保证offset不会重叠
    RetCode DoorPlateImp::Append(const char* key, const uint64_t& offset) {
        // size_维护当前最大的偏移值
        // ssize_t r = pwrite(fd_, key, kKeySize, offset * kKeySize);
        // if(r == -1) {
        //     printf("pwrite(fd_=%d, size_=%lu) failed\n", fd_, size_);
        //     return kIOError;
        // }
        // size_++;
        memcpy(index_ptr+offset*kKeySize,key,kKeySize);
        // memcpy(index_ptr+offset*12+8,offset,4);

        return kSucc;
    }

    // 查找 key 所对应的偏移值
    RetCode DoorPlateImp::Find(const uint64_t& key, uint64_t *offset) {
        
        // Item target(key, 0);
        // std::vector<Item>::iterator it = std::lower_bound(items_.begin(), items_.end(), target, SearchCmp);

        // // 找不到 key
        // if(it == items_.end()) { return kNotFound; }

        // // 判断是否是要找的key
        // if(it->key != key) { return kNotFound; }

        // *offset = it->offset;
        // return kSucc;
        int s=0,e=unique_count-1;
        int mid;
        while(s<=e)
        {
            mid=(s+e)/2;
            uint64_t thiskey=items_[mid].key;
            if(thiskey==key)
            {
                *offset=items_[mid].offset;
                return kSucc;
            }
            else if(thiskey<key)
                s=mid+1;
            else
                e=mid-1;
        }
        return kNotFound;
     
    }

    std::vector<Item>& DoorPlateImp::Data() {
        return items_;
    }

    DoorPlateImp::~DoorPlateImp() {
        // printf("~DoorPlateImp called\n");
        if (fd_ > 0) {
            close(fd_);
        }
        if(index_ptr!=NULL)
        {
             munmap(index_ptr,mapsize_);
        }
        // items_.clear();
        // items_.shrink_to_fit();
        // items_.~vector();
    }

} // namespace polar_race