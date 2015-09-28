/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/param.h>
#include <unistd.h>

//#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "mdbm.h"
#include "mdbm_internal.h"

#include "bench_data_utils.hh"

static const uint32_t ALLOCATE_EXTRA_BYTES = 10240;
static uint32_t TotalOpsCount  = 2000000;  // Run 2M fetches/writes by default

static uint32_t MaxValueSize = 0;

enum {
    MDBM_NO_LOCK = 0,
    MDBM_LOCK = 1,
    MDBM_PLOCK = 2,
    MDBM_RWLOCK = 3
};

/* Doesn't do anything in mdbm_bench executable, just a hook for anyone #including the mdbm_bench code */
extern "C" {
void post_open_callback(MDBM *db) { }
}

int
benchmarkExisting(const char *filename, double percentWrites, int lockmode, 
                  const char *outputfile, uint opCount, int verbose)
{
    if (opCount) {
        TotalOpsCount = opCount;
    }
    srandom(9991);  // An arbitrary prime
    FILE *ofile = stdout;
    if ((outputfile != NULL) && outputfile[0]) {
        ofile = fopen(outputfile, "w+");
    } 

    int flags = MDBM_O_RDONLY;
    if (percentWrites > 0.0) {
        flags = MDBM_O_RDWR;
    }
    int statflags = 0;
    if (lockmode == MDBM_NO_LOCK) {
        flags |= MDBM_NO_LOCK;
        statflags |= MDBM_STAT_NOLOCK;
    } else if (lockmode == MDBM_PLOCK) {
        flags |= MDBM_PARTITIONED_LOCKS;
    } else if (lockmode == MDBM_RWLOCK) {
        flags |= MDBM_RW_LOCKS;
    }

    MDBM *db = mdbm_open(filename, flags, 0666, 0, 0);
    if (db == NULL) {
        fprintf(ofile, "BENCH ERROR: Unable to benchmark MDBM file %s\n", filename);
        fclose(ofile);
        return -1;
    }

    std::string curfile(filename);
    std::vector<std::string> datafiles;
    FileStorage writer(curfile);
    {
        RandomKeyBatch keyBatch(curfile);
        std::vector<datum> keys;
        bool doContinue;
        do {
            doContinue = keyBatch.getKeyBatch(keys);
            std::string dfile(writer.writeData(keys));
            if (dfile.empty()) {
                if (ofile != NULL) {
                    fprintf(ofile, "BENCH ERROR: Unable to write to %s\n", dfile.c_str());
                }
                break;
            } else if (verbose) {
                fprintf(ofile, "BENCH: creating data file %s\n", dfile.c_str());
            }
            datafiles.push_back(dfile);
            keys.clear();
        }
        while (doContinue);
    }  // keyBatch should go out of scope to close the file
    random_shuffle(datafiles.begin(), datafiles.end());

    // MaxValueSize has a 10K padding
    uint32_t largestValue;

    if (!MaxValueSize) {
        mdbm_db_info_t dbinfo;
        mdbm_stat_info_t dbstats;
        if (mdbm_get_db_stats(db,&dbinfo,&dbstats,statflags) < 0) {
            fprintf(ofile, "BENCH ERROR: mdbm_get_db_stats did not get maximum size\n");
            fclose(ofile);
            return -1;
        }
        largestValue = MAX(dbstats.max_val_bytes, dbstats.max_lob_bytes);
        if (!largestValue) {
            fprintf(ofile, "BENCH ERROR: mdbm_get_db_stats returned zero maximum size\n");
            fclose(ofile);
            return -1;
        }
        MaxValueSize = largestValue + ALLOCATE_EXTRA_BYTES;
    } else if (MaxValueSize <= ALLOCATE_EXTRA_BYTES) {
            fprintf(ofile, "BENCH ERROR: Incorrectly set MaxValueSize\n");
            fclose(ofile);
            return -1;
    } else {
        largestValue = MaxValueSize - ALLOCATE_EXTRA_BYTES;
    }

    char *buffer = static_cast<char *> (malloc(MaxValueSize));
    datum bufd;
    bufd.dptr = buffer;
    bufd.dsize = MaxValueSize;

    uint64_t endt, startt = get_time_usec();
    int ret = 0;
    uint64_t totalTimeFetch = 0, totalTimeWrite = 0;
    long writefreq = lround(percentWrites * TotalOpsCount / 100.0);
    if (writefreq) {
        writefreq = TotalOpsCount / writefreq;
    }
    uint readcount = 0, writecount = 0, failcount = 0;
    for (uint i = 0; i < datafiles.size() && failcount < 2; ++i) {
        std::vector<datum> keys;
        writer.readData(datafiles[i], keys);  // Read key data
        for (uint j = 0; j < TotalOpsCount && failcount < 2; ++j) {
            datum value;
            datum key = keys[j % keys.size()];
            bool doWrite = false;
            if ((writefreq > 0) && ((j % writefreq) == 0)) {
                doWrite = true;
            }
            uint64_t t0 = get_time_usec();
            // Perform benchmarks on keys
            if (doWrite) {
                ++writecount;
                value.dptr = bufd.dptr;
                value.dsize = 1 + (random() % (largestValue-1));
                ret = mdbm_store(db, key, value, MDBM_REPLACE);
                totalTimeWrite += (get_time_usec() - t0);
            } else {   // Perform a read
                ++readcount;
                ret = mdbm_fetch_buf(db, &key, &value, &bufd, 0);  // fetch_buf does locking
                totalTimeFetch += (get_time_usec() - t0);
            }
            bufd.dsize = MAX(MaxValueSize, (uint) bufd.dsize);
            if (ret < 0) {
                char *ptr = bufd.dptr +
                            snprintf(bufd.dptr, bufd.dsize, 
                                    "BENCH ERROR: Failed to %s key #%d (len=%d): ",
                                    (doWrite ? "write" : "fetch"), j, key.dsize); 
                for (int k = 0; k < key.dsize; ++k) {
                    if (!key.dptr[k]) {
                        *ptr++ = '\\';
                        *ptr++ = '0';
                    } else {
                        *ptr++ = key.dptr[k];
                    }
                }
                *ptr++ = '\n';
                *ptr = '\0';
                fputs(bufd.dptr, ofile);
                ++failcount;
            }
        }
        writer.dropData(keys);
        if ( unlink(datafiles[i].c_str()) != 0) {
            fprintf(ofile, "BENCH: Failed to remove data file %s\n", datafiles[i].c_str()); 
        }
    }

    if (verbose) {
        endt = get_time_usec();
        fprintf(ofile, "BENCH: Usec delta= %llu #reads= %u #writes= %u Tfetch= %llu Twrite=%llu\n", 
                (long long unsigned)(endt - startt), readcount, writecount, 
                (long long unsigned)totalTimeFetch, (long long unsigned)totalTimeWrite); 
    }

    free(bufd.dptr);
    mdbm_close(db);
    // Generate output similar to mdbm_bench
    double tmp = readcount * 1000000.0;
    uint32_t fetchesPerSec = lround(totalTimeFetch ? (tmp / totalTimeFetch) : 0);
    tmp = writecount * 1000000.0; 
    uint32_t writesPerSec = lround(totalTimeWrite ? (tmp / totalTimeWrite) : 0);

    fprintf(ofile, 
            "nproc  fetch/s      msec  store/s      msec    del/s      msec  getpg/s      msec\n");
    fprintf(ofile, "    1 %8u %9.5f %8u %9.3f %8u %9.3f %8u %9.3f\n", fetchesPerSec, 0.0,
            writesPerSec, 0.0, 0, 0.0, 0, 0.0 ); 
    if ((outputfile != NULL) && outputfile[0] && (fclose(ofile) != 0)) {
        if (ofile == NULL)
	   return 0; 
        fprintf(stderr, "BENCH: Failed to close output file\n"); 
    }
    return 0;
}

