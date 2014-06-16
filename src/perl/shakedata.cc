/*  Copyright 2013 Yahoo! Inc.                                         */
/*  See LICENSE in the root of the distribution for licensing details. */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "mdbm.h"

#include <set>
#include "shakedata.h"

datum
getPageEntryKey(struct mdbm_shake_data_v3 *shakeinfo, unsigned int index)
{
    unsigned int i = 0;
    kvpair *cur = shakeinfo->page_items;

    if (index > shakeinfo->page_num_items) {
        const datum empty = { NULL, 0};
        return empty;
    }

    for(; i < index; ++i) {
        ++cur;
    }
    return cur->key;
}

datum
getPageEntryValue(struct mdbm_shake_data_v3 *shakeinfo, unsigned int index)
{
    unsigned int i = 0;
    kvpair *cur = shakeinfo->page_items;

    if (index > shakeinfo->page_num_items) {
        const datum empty = { NULL, 0};
        return empty;
    }

    for(; i < index; ++i) {
        ++cur;
    }
    return cur->val;
}

unsigned int
getCount(struct mdbm_shake_data_v3 *shakeinfo)
{
    return shakeinfo->page_num_items;
}

unsigned int
getPageNum(struct mdbm_shake_data_v3 *shakeinfo)
{
    return shakeinfo->page_num;
}

unsigned int getFreeSpace(struct mdbm_shake_data_v3 *shakeinfo)
{
    return shakeinfo->page_free_space;
}

unsigned int getSpaceNeeded(struct mdbm_shake_data_v3 *shakeinfo)
{
    return shakeinfo->space_needed;
}

void
setEntryDeleted(struct mdbm_shake_data_v3 *shakeinfo, unsigned int index)
{
    unsigned int i = 0;
    kvpair *cur = shakeinfo->page_items;

    for(; i < index; ++i) {
        ++cur;
    }
    cur->key.dsize = 0;
}

using namespace std;

static set<void *> PtrSet;

void
AddPtr(void *ptr)
{
    PtrSet.insert(ptr);
}

// Returns NULL if not found, delete otherwise and return ptr
void *
DeleteExistingPtr(void *ptr)
{
    if (PtrSet.find(ptr) == PtrSet.end()) {
        return NULL;
    }
    PtrSet.erase(ptr);
    return ptr;
}
