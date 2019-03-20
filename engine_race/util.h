// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_UTIL_H_
#define ENGINE_RACE_UTIL_H_
#include <stdint.h>
#include <pthread.h>
#include <string>
#include <vector>
#include <aio.h>

namespace polar_race {

    // 二分查找
    int64_t BinarySearch(const uint64_t arr[], int64_t start, int64_t end, uint64_t key);

    // 8字节字符串key转无符号64整形
    uint64_t ByteToUnsignedInt64(const char* bytes);

    // memcpy得到的偏移转为无符号32整形
    uint32_t ByteToUnsignedInt32(const char* bytes);

    // Hash
    uint32_t StrHash(const char* s, int size);

    // Env
    int GetDirFiles(const std::string& dir, std::vector<std::string>* result);

    uint32_t GetFileLength(const std::string& file);

    int FileAppend(int fd, const std::string& value);

    bool FileExists(const std::string& path);

    // FileLock
    class FileLock  {
        public:
            FileLock() {}
            virtual ~FileLock() {}

            int fd_;
            std::string name_;

        private:
            // No copying allowed
            FileLock(const FileLock&);
            void operator=(const FileLock&);
    };

    int LockFile(const std::string& f, FileLock** l);
    int UnlockFile(FileLock* l);
    
}  // namespace polar_race

#endif
