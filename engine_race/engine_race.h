// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <string>
#include <thread>
#include <atomic>
#include <pthread.h>

#include <sparsepp/spp.h>

#include "include/engine.h"
#include "util.h"
#include "data_store.h"
#include "door_plate.h"
#include "naive_range.h"

namespace polar_race {

    class EngineRace: public Engine  {
        public:
            static RetCode Open(const std::string& name, Engine** eptr);
            explicit EngineRace(const std::string& name) { }

            ~EngineRace();

            RetCode Write(const PolarString& key, const PolarString& value) override;
            RetCode Read(const PolarString& key, std::string* value) override;
            RetCode Range(const PolarString& lower, const PolarString& upper, Visitor &visitor) override;

        private:
            DataStore *data_;
            DoorPlate *door_;
            NaiveRange *rangetask;

            std::mutex thread_lock;
            int thread_index;
            int totalsum;
            std::chrono::high_resolution_clock::time_point starttime;
            

    };

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
