// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_NAIVE_RANGE_H_
#define ENGINE_NAIVE_RANGE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>
#include<condition_variable>

#include "include/engine.h"
#include "data_store.h"
#include "door_plate.h"
#include "util.h"
#include "include/polar_string.h"
#include "para.h"



namespace polar_race {



    class NaiveRange {
        public:
            explicit NaiveRange(const std::string& path, const int& max_open);
            
            ~NaiveRange();

            RetCode Init();

   

            RetCode rangeByBuffer(Visitor &visitor, DoorPlate* door, DataStore* data);

            RetCode visitBlock(int file_index,int block_index,Visitor &visitor, DoorPlate* door, DataStore* data);

            // RetCode writeBlock(int index,DoorPlate* door, DataStore* data);

            RetCode loadFile(int fd,int ind);

            RetCode competeBlock(DoorPlate* door, DataStore* data);


        private:
            std::string path;
            int max_open;
            int cur_index;
            int readcount[FILE_NUM];

            bool buffer_ready[FILE_NUM];
            bool readcomplete[FILE_NUM*2];
            std::mutex mtx;
            
            
            char * rangeBuffer;

            std::mutex *block_mtx[BLOCK_NUM*2];
            std::mutex *read_mtx[BLOCK_NUM*2];

            std::condition_variable* block_cv_r[BLOCK_NUM*2];
            std::condition_variable* block_cv_w[BLOCK_NUM*2];
            
            uint64_t buffersize;

    };

}  // namespace polar_race

#endif  // ENGINE_NAIVE_RANGE_H_
