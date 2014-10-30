/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

/**
 * Flags that can be passed to mdbm_open
 * 
 * @author areese
 * 
 */
public class Open {

    /*
     * mdbm_open flags
     */

    /**
     * MDBM_O_RDONLY open for reading only
     */
    public static final int MDBM_O_RDONLY = 0x00000000;

    /**
     * MDBM_O_WRONLY same as RDWR (deprecated)
     */
    @Deprecated
    public static final int MDBM_O_WRONLY = 0x00000001;

    /**
     * MDBM_O_RDWR open for reading and writing
     */
    public static final int MDBM_O_RDWR = 0x00000002;

    /**
     * 
     */
    public static final int MDBM_O_ACCMODE = (MDBM_O_RDONLY | MDBM_O_WRONLY | MDBM_O_RDWR);

    /**
     * MDBM_O_CREAT create file if it does not exist
     */
    public static final int MDBM_O_CREAT = 0x00000040;

    /**
     * MDBM_O_TRUNC truncate size to 0
     */
    public static final int MDBM_O_TRUNC = 0x00000200;

    /**
     * MDBM_O_FSYNC sync mdbm upon mdbm_close()
     */
    public static final int MDBM_O_FSYNC = 0x00001000;

    /**
     * MDBM_O_ASYNC enable asynchronous sync'ing by the kernel syncer process.
     */
    public static final int MDBM_O_ASYNC = 0x00002000;

    /**
     * MDBM_O_DIRECT Use O_DIRECT when accessing backing-store files
     */
    public static final int MDBM_O_DIRECT = 0x00004000;

    /**
     * MDBM_SINGLE_ARCH user *promises* not to mix 32/64-bit access Only works on RHEL6 with MDBM4, and only if all
     * processes accessing the mdbm are 64bit or 32bit
     */
    public static final int MDBM_SINGLE_ARCH = 0x00080000;

    /**
     * MDBM_NO_DIRTY do not not track clean/dirty status
     */
    public static final int MDBM_NO_DIRTY = 0x00010000;

    /**
     * MDBM_OPEN_WINDOWED_MIN use windowing (minimum footprint)
     */
    public static final int MDBM_OPEN_WINDOWED_MIN = 0x00020000;

    /**
     * MDBM_OPEN_WINDOWED use windowing to access db, only available with MDBM_O_RDWR
     */
    public static final int MDBM_OPEN_WINDOWED = 0x00100000;

    /**
     * MDBM_PROTECT protect database except when locked
     */
    public static final int MDBM_PROTECT = 0x00200000;

    /**
     * MDBM_DBSIZE_MB dbsize is specific in MB
     */
    public static final int MDBM_DBSIZE_MB = 0x00400000;

    /**
     * MDBM_LARGE_OBJECTS support large objects - obsolete
     */
    public static final int MDBM_LARGE_OBJECTS = 0x01000000;

    /**
     * MDBM_PARTITIONED_LOCKS partitioned locks
     */
    public static final int MDBM_PARTITIONED_LOCKS = 0x02000000;

    /**
     * MDBM_RW_LOCKS read-write locks
     */
    public static final int MDBM_RW_LOCKS = 0x08000000;

    /**
     * MDBM_CREATE_V3 create a V3 db
     */
    public static final int MDBM_CREATE_V3 = 0x20000000;

    /**
     * MDBM_HEADER_ONLY map header only (internal use)
     */
    // public static final int MDBM_HEADER_ONLY = 0x40000000;/* map header only
    // (internal use) */

    /**
     * MDBM_OPEN_NOLOCK don't lock during open
     */
    public static final int MDBM_OPEN_NOLOCK = 0x80000000;

    // #define MDBM_DEMAND_PAGING 0x04000000 /* (v2 only) */
    // #define MDBM_DBSIZE_MB_OLD 0x04000000 /* (don't use -- conflicts with

}
