// Copyright [2018] Alibaba Cloud All rights reserved
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>

#include "util.h"

namespace polar_race {

    static const int kA = 54059;  // a prime
    static const int kB = 76963;  // another prime
    static const int kFinish = 37;  // also prime

    // 把字符串转为 uint32
    uint32_t ByteToUnsignedInt32(const char* bytes) {
        uint32_t ret = ((uint32_t)bytes[0] & 0xff);
        ret |= (((uint32_t)bytes[1] << 8) & 0xff00);
        ret |= (((uint32_t)bytes[2] << 16) & 0xff0000);
        ret |= (((uint32_t)bytes[3] << 24) & 0xff000000);
        return ret;
    }

    uint64_t ByteToUnsignedInt64(const char* bytes) {
        uint64_t ret = (bytes[7] & 0xff);
        ret |= (((uint64_t)bytes[6] << 8) & 0xff00);
        ret |= (((uint64_t)bytes[5] << 16) & 0xff0000);
        ret |= (((uint64_t)bytes[4] << 24) & 0xff000000);
        ret |= (((uint64_t)bytes[3] << 32) & 0xff00000000);
        ret |= (((uint64_t)bytes[2] << 40) & 0xff0000000000);
        ret |= (((uint64_t)bytes[1] << 48) & 0xff000000000000);
        ret |= (((uint64_t)bytes[0] << 56) & 0xff00000000000000);
        return ret;
    }

    // 返回值-1表示没找到
    int64_t BinarySearch(const uint64_t arr[], int64_t start, int64_t end, uint64_t key) {
        int64_t ret = -1;
        int64_t mid;
        while (start <= end) {
            mid = start + ((end - start) >> 1);
            if (arr[mid] < key)
                start = mid + 1;
            else if (arr[mid] > key)
                end = mid - 1;
            else {
                ret = mid;
                break;
            }
        }
        return ret;
    }

    uint32_t StrHash(const char* s, int size) {
        uint32_t h = kFinish;
        while (size > 0) {
            h = (h * kA) ^ (s[0] * kB);
            s++;
            size--;
        }
        return h;
    }

    int GetDirFiles(const std::string& dir, std::vector<std::string>* result) {
        int res = 0;
        result->clear();
        DIR* d = opendir(dir.c_str());
        if (d == NULL) {
            return errno;
        }
        struct dirent* entry;
        while ((entry = readdir(d)) != NULL) {
            if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
                continue;
            }
            result->push_back(entry->d_name);
        }
        closedir(d);
        return res;
    }

    uint32_t GetFileLength(const std::string& file) {
        struct stat stat_buf;
        int rc = stat(file.c_str(), &stat_buf);
        return rc == 0 ? stat_buf.st_size : -1;
    }

    int FileAppend(int fd, const std::string& value) {
        if (fd < 0) {
            return -1;
        }
        size_t value_len = value.size();
        const char* pos = value.data();
        while (value_len > 0) {
            ssize_t r = write(fd, pos, value_len);
            if (r < 0) {
                if (errno == EINTR) {
                    continue;  // Retry
                }
                return -1;
            }
            pos += r;
            value_len -= r;
        }
        return 0;
    }

    bool FileExists(const std::string& path) {
        return access(path.c_str(), F_OK) == 0;
    }

    static int LockOrUnlock(int fd, bool lock) {
        errno = 0;
        struct flock f;
        memset(&f, 0, sizeof(f));
        f.l_type = (lock ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0;
        return fcntl(fd, F_SETLK, &f);
    }

    int LockFile(const std::string& fname, FileLock** lock) {
        *lock = NULL;
        int result = 0;
        int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0) {
            result = errno;
        } else if (LockOrUnlock(fd, true) == -1) {
            result = errno;
            close(fd);
        } else {
            FileLock* my_lock = new FileLock;
            my_lock->fd_ = fd;
            my_lock->name_ = fname;
            *lock = my_lock;
        }
        return result;
    }

    int UnlockFile(FileLock* lock) {
        int result = 0;
        if (LockOrUnlock(lock->fd_, false) == -1) {
            result = errno;
        }
        close(lock->fd_);
        delete lock;
        return result;
    }

}  // namespace polar_race