/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM3_STATS_H_ONCE
#define MDBM3_STATS_H_ONCE

#include <stdint.h>

#include <mdbm.h>

#ifdef  __cplusplus
extern "C" {
#endif

#define MDBM_RSTATS_VERSION     0x53090004
#define MDBM_RSTATS_THIST_MAX (6*9+2)

struct mdbm_rstats_val {
    uint64_t    num;
    uint64_t    num_error;
    uint64_t    sum_usec;
    uint64_t    num_lock_wait;
    uint64_t    sum_lock_wait_usec;

    uint64_t    thist[MDBM_RSTATS_THIST_MAX];
};
typedef struct mdbm_rstats_val mdbm_rstats_val_t;

struct mdbm_rstats {
    uint32_t    version;
    uint16_t    flags;
    uint16_t    reserved0;

    uint32_t    lock;
    uint32_t    reserved1;

    mdbm_rstats_val_t   fetch;
    mdbm_rstats_val_t   fetch_uncached;
    mdbm_rstats_val_t   store;
    mdbm_rstats_val_t   remove;

    mdbm_rstats_val_t   getpage;
    mdbm_rstats_val_t   getpage_uncached;
    mdbm_rstats_val_t   cache_evict;
    mdbm_rstats_val_t   cache_store;
};
typedef struct mdbm_rstats mdbm_rstats_t;

#define MDBM_RSTATS_THIST       0x01

struct mdbm_rstats_mem;

int mdbm_reset_rstats (struct mdbm_rstats* rs);
int mdbm_init_rstats (MDBM* db, int flags);
int mdbm_open_rstats (const char* dbfilename, int flags,
                      struct mdbm_rstats_mem** mem, struct mdbm_rstats** rstats);
int mdbm_close_rstats (struct mdbm_rstats_mem* mem);

void mdbm_diff_rstats (mdbm_rstats_t* base, mdbm_rstats_t* sample, mdbm_rstats_t* diff,
                       mdbm_rstats_t* new_base);


#ifdef  __cplusplus
}
#endif

#endif  /* MDBM3_STATS_H_ONCE */
