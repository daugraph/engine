// Copyright [2018] Alibaba Cloud All rights reserved
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <map>
#include <cstring>
#include <iostream>
#include <utility>
#include <unistd.h>
#include <mutex>

#include "util.h"
#include "data_store_imp.h"
#include "door_plate_imp.h"

namespace polar_race {
    
    static const char kDataFileNamePrefix[] = "DATA_";
    static const char kDataCachePrefix[] = "Cache_";

    static const uint64_t kBlockSize = 4096;

    DataStoreImp::DataStoreImp(const std::string& dir, const int& file_no)
        :dir_(dir), fd_(-1), file_no_(file_no), size_(0) {
            file_interval=67584000/FILE_NUM *BLOCK_SIZE;
            std::string path  = dir_ + "/" + kDataFileNamePrefix + std::to_string(file_no_);
            bool exist=FileExists(path);
            fd_=open(path.c_str(), O_RDWR | O_CREAT | O_DIRECT|O_NOATIME , 0655);
            if(exist)
                fallocate(fd_,0,0,file_interval*DIV_PART);
        }

    // RetCode DataStoreImp::Init (DoorPlate *door) {
    //     mtx_ = new std::mutex();
    //     posix_memalign ((void**)&cache_buf, BLOCK_SIZE, CACHE_SIZE*BLOCK_SIZE);
        
    //     std::string path = dir_ + "/" + kDataFileNamePrefix + std::to_string(file_no_);
    //     auto items=door->Data();
    //     size_ = items[file_no_]->size_;

    //     bool exist=FileExists(path);
    //     int fd=open(path.c_str(), O_RDWR | O_CREAT | O_DIRECT|O_NOATIME , 0655);
    //     // writecount=size_-GetFileLength(path)/BLOCK_SIZE;
    //     // printf("write count:%d\n",writecount);
    //     writecount=size_%CACHE_SIZE;
    //     fd_ = fd;
    //     // if(exist)
    //     //     fallocate(fd_,0,0,67584000/FILE_NUM);
    //     // path=dir_+"/"+kDataCachePrefix+std::to_string(file_no_);
    //     // int fd1=open(path.c_str(), O_RDWR | O_CREAT| O_NOATIME, 0655);
    //     // if(ftruncate(fd1, CACHE_SIZE*BLOCK_SIZE)<0)
    //     //     return kIOError;
    //     // write_buf=(char*)mmap(NULL, CACHE_SIZE*BLOCK_SIZE, PROT_READ | PROT_WRITE , MAP_SHARED|MAP_POPULATE | MAP_NONBLOCK,  fd1, 0);
    //     write_buf=items[file_no_]->index_ptr+(64000000/FILE_NUM+5000)*8;
    //     write_pos=0;
    //     // RecoverFile();
    //     return kSucc;
    // }
    RetCode DataStoreImp::Init (DoorPlate *door) {
        
        auto items=door->Data();
        memset(write_pos,0,sizeof(write_pos));
        for(int i=0;i<DIV_PART;i++)
        {
            int interval=FILE_NUM/DIV_PART;
            
            file_size[i]=items[interval*i+file_no_]->size_;
            writecount[i]=file_size[i]%CACHE_SIZE;
            posix_memalign ((void**)&cache_buf[i], BLOCK_SIZE, CACHE_SIZE*BLOCK_SIZE);
            write_buf[i]=items[interval*i+file_no_]->index_ptr+(64000000/FILE_NUM+5000)*8;
        }
       
        
        // RecoverFile();
        return kSucc;
    }
    // RetCode DataStoreImp::RecoverFile(){
        
    //     if(writecount!=0)
    //     {            
    //         memcpy(cache_buf,write_buf,kBlockSize*writecount);
    //         if(pwrite(fd_,cache_buf,kBlockSize*writecount,(size_-writecount)*kBlockSize)<0)
    //             return kIOError;
    //     }
    //     return kSucc;
            
    // }
    RetCode DataStoreImp::RecoverFile(){
        for(int i=0;i<DIV_PART;i++)
        {
            if(writecount[i]!=0)
            {            
                memcpy(cache_buf[i],write_buf[i],kBlockSize*writecount[i]);
                if(pwrite(fd_,cache_buf[i],kBlockSize*writecount[i],file_interval*i+(file_size[i]-writecount[i])*kBlockSize)<0)
                    return kIOError;
            }   
        }  
        return kSucc;
    }


    // RetCode DataStoreImp::AppendCache(const char* data, uint64_t* offset)
    // {   
    //     mtx_->lock();
    //     *offset=size_;
    //     // printf("enter into read : %lu\n",cur_offset);
    //     size_++;
    //     memcpy(write_buf+write_pos*kBlockSize,data,kBlockSize);
    //     write_pos=(write_pos+1)%CACHE_SIZE;
    //     if(write_pos==0)
    //     {
    //         memcpy(cache_buf,write_buf,kBlockSize*CACHE_SIZE);
    //         if(pwrite(fd_, cache_buf, CACHE_SIZE*kBlockSize, (size_-CACHE_SIZE) * kBlockSize)<0)
    //             return kIOError;
    //     }
    //     mtx_->unlock();
       
    //     return kSucc;
    // }
    RetCode DataStoreImp::AppendCache(const char* data, uint64_t* offset,int index)
    {   
        mtx_[index].lock();
        *offset=file_size[index];
        // printf("enter into read : %lu\n",cur_offset);
        file_size[index]++;
        memcpy(write_buf[index]+write_pos[index]*kBlockSize,data,kBlockSize);
        write_pos[index]=(write_pos[index]+1)%CACHE_SIZE;
        if(write_pos[index]==0)
        {
            memcpy(cache_buf[index],write_buf[index],kBlockSize*CACHE_SIZE);
            if(pwrite(fd_, cache_buf[index], CACHE_SIZE*kBlockSize, file_interval*index+(file_size[index]-CACHE_SIZE) * kBlockSize)<0)
                return kIOError;
        }
        mtx_[index].unlock();
        return kSucc;
    }

    // 读取 offset 位置的数据
    // RetCode DataStoreImp::Read(const uint64_t& offset, char* buf) {
    //     // 读取 4KB        
    //     // ssize_t r = pread(fd_, buf, kBlockSize, offset * kBlockSize);
    //     // if(r == -1) {
    //     //     printf("pread(fd_=%d, cur=%lu) failed\n", fd_, offset);
    //     //     return kIOError;
    //     // }
    //     if(pread(fd_, buf, kBlockSize, offset * kBlockSize)<0)
    //         return kIOError;
    //     return kSucc;
    // }

    RetCode DataStoreImp::Read(const uint64_t& offset, char* buf, int index) {

        if(pread(fd_, buf, kBlockSize, file_interval*index+offset * kBlockSize)<0)
            return kIOError;
        return kSucc;
    }

    uint64_t DataStoreImp::getSize(const int& index){ return file_size[index];}
    int DataStoreImp::getFd(){return fd_;}


    DataStoreImp::~DataStoreImp() {
   
        if (fd_ > 0) {
            close(fd_);
        }

        // if(mtx_ != NULL) {
        //     delete mtx_;
        // }
        for(int i=0;i<DIV_PART;i++)
            free(cache_buf[i]);
    }

}  // namespace polar_race