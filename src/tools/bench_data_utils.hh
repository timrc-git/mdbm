/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM_BENCH_DATA_UTILS
#define MDBM_BENCH_DATA_UTILS

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>

#include "mdbm.h"

    
extern "C" {
int benchmarkExisting(const char *filename, double percentWrites, int lockmode, 
                       const char *outputfile, uint opCount, int verbose);
}   

class FileStorage
{

public:

    FileStorage(const std::string &fname) : 
                filename_(fname), count_(0), map_(NULL), mapsize_(0)
    {
    }

    ~FileStorage() { }

    // Write data vector to disk, returning the file name (constructor's filename + __##)
    std::string
    writeData(std::vector<datum> &dataVec)
    {
        std::ostringstream fn;

        fn << filename_ << "__" << count_ << ".DVs";
        std::string fname(fn.str());

        int fd = open(fname.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);

        if (fd < 0) {
            std::cerr << "Unable to open for write, file= " << fname << std::endl;
            return "";
        }

        for (uint i = 0; i < dataVec.size(); ++i) {
            if (write(fd, &dataVec[i].dsize, sizeof(int)) < 0) {
                std::cerr << "Unable to write size: " << fname << std::endl;
            }
            if (write(fd, dataVec[i].dptr, dataVec[i].dsize) < 0) {
                std::cerr << "Unable to write data: " << fname << std::endl;
            }
        }

        if (close(fd) != 0) {
            std::cerr << "Unable to close file: " << fname << std::endl;
        }
        ++count_;
        return fname;
    }

    bool 
    readData(std::string const &fname, std::vector<datum> &dataVec)
    {
        int fd = open(fname.c_str(), O_RDONLY, 0444);

        if (fd < 0) {
            std::cerr << "Unable to open data file: " << fname << std::endl;
            return false;
        }

        struct stat filestat;
        if (fstat(fd, &filestat) < 0) {
            std::cerr << "Unable to fstat data file: " << fname << std::endl;
            return false;
        }
        mapsize_ = filestat.st_size;

        map_ = (char *) mmap(NULL, mapsize_, PROT_READ, MAP_SHARED, fd, 0);
        if (map_ == MAP_FAILED) {
            std::cerr << "Unable to memory map data file: " << fname << std::endl;
            return false;
        }

        datum dta;
        char *cur = map_;
        while ((cur - map_) < mapsize_) {
            dta.dsize = *(reinterpret_cast<int *>(cur));
            if (dta.dsize <= 0) {
                std::cerr << "Invalid size in file: " << fname << ", at offset " << (cur - map_) 
                          << std::endl;
                continue;
            }
            cur += sizeof(int);
            dta.dptr = cur;
            dataVec.push_back(dta);
            cur += dta.dsize;
        }
        return true;
    }

    void
    dropData(std::vector<datum> &dataVec)
    {
        dataVec.clear();
        if ((map_ != NULL) && mapsize_ != 0) {
            if (munmap(map_, mapsize_) != 0) {
                std::cerr << "Unable to unmap memory map" << std::endl;
            }
        }
    }

private:
    std::string filename_;
    uint count_;
    char *map_;
    int64_t mapsize_;

}; // FileStorage Class definition

// ** Definitions

static const int MB = 1024 * 1024;
static const datum emptyDatum = { NULL, 0 };

class RandomKeyBatch
{

public:

    // Open an MDBM RDONLY and get the first key
    datum
    getFirstKey()
    {
        db_ = mdbm_open(filename_.c_str(), MDBM_O_RDONLY | MDBM_OPEN_NOLOCK, 0444, 0, 0);
        if (db_ == NULL) {
             std::cerr << "Cannot open DB " << filename_ << " to get first key" << std::endl;
             return emptyDatum;
        }
        return mdbm_firstkey_r(db_, &iter_);
    }

    RandomKeyBatch(const std::string &fname, uint topSize = 256 * MB)
        : filename_(fname), maxSize_(topSize) 
    {
        firstTime_ = true;
        db_ = NULL;
    }

    ~RandomKeyBatch()
    {
        if (db_) {
            mdbm_close(db_);
            db_ = NULL;
        }
    }

    // Returns true if there is more key data to be read in MDBM, otherwise false
    bool
    getKeyBatch(std::vector<datum> &keyBatch)
    {
        datum key;
        key.dptr  = (char*)"";
        key.dsize = strlen(key.dptr);

        if (firstTime_) {
            key = getFirstKey();
            firstTime_ = false;
        }

        if (key.dsize == 0) {
            std::cerr << "Too few keys in DB" << std::endl;
            return false;
        } 

        uint size = 0;
        do {
            keyBatch.push_back(key);
            size += key.dsize;
            key = mdbm_nextkey_r(db_, &iter_);
        } while ((key.dsize != 0) && (size < maxSize_));

        random_shuffle(keyBatch.begin(), keyBatch.end());

        return (key.dsize != 0);
    }  // getKeyBatch

private:
    std::string filename_;
    MDBM *db_;
    MDBM_ITER iter_;
    bool firstTime_;
    uint maxSize_;

}; // RandomKeyBatch Class definition

#endif
