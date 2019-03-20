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
#include <stdlib.h>
#include <mutex>
#include <thread>

#include "naive_range.h"




namespace polar_race {
    static const char kDataFileNamePrefix[] = "DATA_";

    NaiveRange::NaiveRange(const std::string& path, const int& max_open)
        :path(path),max_open(max_open),cur_index(0){}
    
     NaiveRange::~NaiveRange(){
        free(rangeBuffer);
     }


    RetCode NaiveRange::Init(){
     
        memset(readcount,0,sizeof(readcount));
        memset(buffer_ready,0,sizeof(buffer_ready));
        memset(readcomplete,0,sizeof(readcomplete));
        buffersize=(67584000/FILE_NUM)*BLOCK_SIZE;
        void * temp;
        if(posix_memalign (&temp, BLOCK_SIZE,buffersize*BLOCK_NUM*2))
            return kIOError;
        rangeBuffer=(char*)temp;
        for(int i=0;i<BLOCK_NUM*2;i++)
        {
            block_mtx[i]=new std::mutex();
            read_mtx[i]=new std::mutex();
            block_cv_r[i]=new std::condition_variable();
            block_cv_w[i]=new std::condition_variable();
           
            // if(posix_memalign (&temp, BLOCK_SIZE, 32500*BLOCK_SIZE))
           
        }      
        return kSucc;
    }

    RetCode NaiveRange::rangeByBuffer(Visitor &visitor, DoorPlate* door, DataStore* data)
    {
        // char *str_key;
        uint64_t temkey;
        auto& indexfile = door->Data();
        int ind;
        for(int i=0;i<max_open;i++)
        {
            // printf("range:%d\n",i);
            if(i<BLOCK_NUM)
            {
                ind=i%BLOCK_NUM+BLOCK_NUM;
                if(!buffer_ready[i])
                {
                    std::unique_lock <std::mutex> lck(*read_mtx[ind]);          
                    while(!buffer_ready[i])
                        block_cv_r[ind]->wait(lck);   
                }             
                auto& items = indexfile[i]->Data();
        
                for(unsigned int in=0;in<indexfile[i]->unique_count;in++) {
                        temkey=__builtin_bswap64(items[in].key);
                        visitor.Visit( PolarString ((char*)(&temkey), 8), PolarString (rangeBuffer+ind*buffersize+(items[in].offset<<12), BLOCK_SIZE));
                }
            }
                
            else{
                ind=i%BLOCK_NUM;
                if(!buffer_ready[i])
                {
                    std::unique_lock <std::mutex> lck(*read_mtx[ind]);          
                    while(!buffer_ready[i])
                        block_cv_r[ind]->wait(lck);   
                }       
                            
                auto& items = indexfile[i]->Data();
        
                for(unsigned int in=0;in<indexfile[i]->unique_count;in++) {
                        temkey=__builtin_bswap64(items[in].key);
                        visitor.Visit( PolarString ((char*)(&temkey), 8), PolarString (rangeBuffer+ind*buffersize+(items[in].offset<<12), BLOCK_SIZE));
                }
                block_mtx[ind]->lock();
                readcount[i]+=1;
                // readcount[ind]-=1;
                // if(readcount[ind]==0)
                if(readcount[i]%64==0)
                {
                    buffer_ready[i]=false;
                    // printf("readcount:%d index:%d\n",readcount[i],i);
                    readcomplete[i+(readcount[i]/64-1)*FILE_NUM]=true;
                    block_cv_w[ind]->notify_all();
                }
                block_mtx[ind]->unlock();
            }

            
        }
        return kSucc;
    }
    RetCode NaiveRange::loadFile(int fd,int index)
    {
        int writeblock=index+BLOCK_NUM;
        ssize_t r = pread(fd, rangeBuffer+buffersize*writeblock, 67584000/FILE_NUM *BLOCK_SIZE, 0);
        if(r<0)
        return kIOError;
        read_mtx[writeblock]->lock();
        buffer_ready[index]=true;
        read_mtx[writeblock]->unlock();
        block_cv_r[writeblock]->notify_all();
        printf("loaded :%d fd:%d\n",index,fd);
        return kSucc;
    }
    // RetCode NaiveRange::writeBlock(int index,DoorPlate* door, DataStore* data)
    // {
    //     int cnt=0;
    //     auto& datafile = data->Data();
    //     while(cnt<256*2)
    //     {
    //         int writeindex=BLOCK_NUM*(cnt%256)+index;
    //         cnt++;
    //         int fd=datafile[writeindex]->getFd();
    //         uint64_t size=datafile[writeindex]->getSize();
    //         // for(int i = 0; i < DIV_PART; i++) {
    //         //     thread_vec.emplace_back(std::thread(&NaiveRange::loadFile,this,fd,index,i,size));
    //         // }
    //         // for(auto& th : thread_vec) th.join();
    //         // thread_vec.clear();
    //         // auto t1 = std::chrono::high_resolution_clock::now();
    //         ssize_t r = pread(fd, rangeBuffer[index], BLOCK_SIZE*size, 0);
    //         if(r<0)
    //             return kIOError;
    //         // auto t2 = std::chrono::high_resolution_clock::now();
    //         // if(index==0)
    //         //     std::cout << "write block takes: "
    //         //               << std::chrono::duration<double, std::milli>(t2 - t1).count()
    //         //               << " milliseconds" << std::endl;
    //         read_mtx[index]->lock();
    //         // printf("buffer: %d into block\n",writeindex);
    //         buffer_ready[writeindex]=true;
    //         read_mtx[index]->unlock();
    //         block_cv_r[index]->notify_all();
    //         {
    //             std::unique_lock <std::mutex> lck(*block_mtx[index]);
    //             while(!readcomplete[BLOCK_NUM*cnt+index])
    //                 block_cv_w[index]->wait(lck);
    //         }
    //         // auto t3 = std::chrono::high_resolution_clock::now();
    //         // if(index==0)
    //         //     std::cout << "waiting read takes: "
    //         //               << std::chrono::duration<double, std::milli>(t3 - t2).count()
    //         //               << " milliseconds" << std::endl;
    //     }
    //     return kSucc;
    // }
    RetCode NaiveRange::competeBlock(DoorPlate* door, DataStore* data)
    {
        int cur_write;
        int writeblock,writefile;
        auto& datafile = data->Data();
        int interval=FILE_NUM/DIV_PART;
        while(1)
        {
            mtx.lock();
            cur_write=cur_index;
            cur_index++;
            // printf("enter into write index:%d\n",cur_index);
            mtx.unlock();
          
            // if(cur_write<BLOCK_NUM || (cur_write>=FILE_NUM && cur_write<FILE_NUM+BLOCK_NUM))
            if(cur_write>=FILE_NUM && cur_write<FILE_NUM+BLOCK_NUM)
                continue;
            if(cur_write<2*FILE_NUM)
            {
                if(cur_write<BLOCK_NUM)
                    writeblock=cur_write+BLOCK_NUM;
                else
                    writeblock=cur_write%BLOCK_NUM;
                writefile=cur_write%FILE_NUM;

                int write_file_no=writefile%interval;
                int write_index=writefile/interval;

                if(cur_write>=BLOCK_NUM+FILE_NUM && cur_write<FILE_NUM+BLOCK_NUM*2 && !readcomplete[cur_write-2*BLOCK_NUM])
                {
                    std::unique_lock <std::mutex> lck(*block_mtx[writeblock]);
                    while(!readcomplete[cur_write-2*BLOCK_NUM])
                        block_cv_w[writeblock]->wait(lck);
                }
                else if(cur_write>=BLOCK_NUM*2 && !readcomplete[cur_write-BLOCK_NUM])
                {
                    std::unique_lock <std::mutex> lck(*block_mtx[writeblock]);
                    while(!readcomplete[cur_write-BLOCK_NUM])
                        block_cv_w[writeblock]->wait(lck);
                }
                int fd=datafile[write_file_no]->getFd();
                uint64_t size=datafile[write_file_no]->getSize(write_index);
                ssize_t r = pread(fd, rangeBuffer+writeblock*buffersize, size*BLOCK_SIZE, 67584000/FILE_NUM*BLOCK_SIZE*write_index);
                if(r<0)
                    return kIOError;
                //read_mtx[writeblock]->lock();
                // printf("buffer: %d into block\n",writeindex);
                buffer_ready[writefile]=true;
                //read_mtx[writeblock]->unlock();
                block_cv_r[writeblock]->notify_all();
            }
            else
            {
                break;
            }
            
        }
        return kSucc;
    }
    RetCode NaiveRange::visitBlock(int file_index,int block_index,Visitor &visitor, DoorPlate* door, DataStore* data){
        
        char str_key[8];
        auto& indexfile = door->Data();
        auto& items = indexfile[file_index]->Data();
        // int ind=block_index%BLOCK_NUM;
        // for(const auto& item : items) {
        //     for(int i = 0; i < 8; i++)
        //         str_key[i] = (char)(item.key >> (56 - 8 * i));     
        //     visitor.Visit( PolarString (str_key, 8), PolarString (rangeBuffer[ind]+(item.offset<<12), BLOCK_SIZE));            
        // }
        for(unsigned int ind=0;ind<indexfile[file_index]->unique_count;ind++) {
                    for(int i = 0; i < 8; i++)
                        str_key[i] = (char)(items[ind].key >> (56 - 8 * i));
                    visitor.Visit( PolarString (str_key, 8), PolarString (rangeBuffer+block_index*buffersize+(items[ind].offset<<12), BLOCK_SIZE));
        }
        return kSucc;
    }

}  // namespace polar_race