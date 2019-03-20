// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_DOOR_PLATE_H_
#define ENGINE_DOOR_PLATE_H_
#include <string.h>
#include <stdint.h>
#include <map>
#include <string>

#include "door_plate_imp.h"
#include "include/engine.h"

namespace polar_race {

    class DoorPlate  {
        public:
            explicit DoorPlate(const std::string& path, const int max_open);
            
            ~DoorPlate();

            RetCode Init();
            
            RetCode Find(const uint64_t& index, const uint64_t& key, uint64_t* offset);

            RetCode Append(const uint64_t& index, const char*key, const uint64_t& offset);

            std::vector<DoorPlateImp*>& Data();
            void index_recover(int index);
            int totalsum;


        private:

            std::string dir_;

            int max_open_;

            

            std::vector<DoorPlateImp*> door_;
    };

}  // namespace polar_race

#endif  // ENGINE_DOOR_PLATE_H_
