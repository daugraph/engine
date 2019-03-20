// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_DATA_STORE_H_
#define ENGINE_DATA_STORE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>

#include "data_store_imp.h"
#include "include/engine.h"


namespace polar_race {

    class DataStore  {
        public:
            explicit DataStore(const std::string& path, const int& max_open);
            
            ~DataStore();

            RetCode Init(DoorPlate *door);
            
            RetCode Read(const uint64_t& index, char *value, const uint64_t& offset);

            RetCode Append(const uint64_t& index, const char *value, uint64_t* offset);

            void Recover_cache(int index, DoorPlate *door);

            std::vector<DataStoreImp*>& Data();
            



        private:
            int datafile_num;
            std::string dir_;

            int max_open_;

            std::vector<DataStoreImp*> data_;
    };

}  // namespace polar_race

#endif