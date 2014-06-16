/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include "mdbm_cxx.h"

bool
MdbmBase::open (const char* filename, int flags, int mode, int pagesize, int dbsize)
{
    db = mdbm_open(filename,flags,mode,pagesize,dbsize);
    return (db != NULL);
}

void
MdbmBase::dbopen (MDBM* d)
{
    db = d;
}

bool
MdbmBase::sync (int flags)
{
    return (mdbm_sync(db) == 0);
}

void
MdbmBase::close()
{
    mdbm_close(db);
    db = NULL;
}


MdbmBase::MdbmBase ()
{
    db = NULL;
}


MdbmBase::~MdbmBase ()
{
    if (db) {
        close();
    }
}


