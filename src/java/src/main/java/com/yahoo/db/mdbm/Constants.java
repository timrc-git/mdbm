/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

class Constants {

    public static final int _MDBM_MAXSPLIT_THIS_PROC = 0x4;

    public static final int _MDBM_ASYNC = 0x10;
    public static final int _MDBM_FSYNC = 0x20;
    public static final int _MDBM_NEWSHAKE = 0x40;
    public static final int _MDBM_DEMAND_PG = 0x80;
    public static final int _MDBM_MEM = 0x8;

    public static final int _MDBM_ALGN16 = 0x1;
    public static final int _MDBM_ALGN32 = 0x3;
    public static final int _MDBM_ALGN64 = 0x7;
    public static final int _MDBM_PERFECT = 0x8;
    public static final int _MDBM_LARGEOBJ = 0x10;
    public static final int _MDBM_MAXSPLIT = 0x20;
    public static final int _MDBM_REPLACED = 0x40;

    /*
     * flags to mdbm_store
     */
    public static final int MDBM_INSERT = 0;
    public static final int MDBM_REPLACE = 1;
    public static final int MDBM_INSERT_DUP = 2;
    public static final int MDBM_MODIFY = 3;
    public static final int MDBM_STORE_MASK = 0x3;

    public static final int MDBM_RESERVE = 0x100;

    /*
     * * Hash functions
     */
    public static final int MDBM_HASH_CRC32 = 0;
    public static final int MDBM_HASH_EJB = 1;
    public static final int MDBM_HASH_PHONG = 2;
    public static final int MDBM_HASH_OZ = 3;
    public static final int MDBM_HASH_TOREK = 4;
    public static final int MDBM_HASH_FNV = 5;
    public static final int MDBM_HASH_STL = 6;
    public static final int MDBM_HASH_MD5 = 7;
    public static final int MDBM_HASH_SHA_1 = 8;
    public static final int MDBM_HASH_JENKINS = 9;

    /* Which hash function to use on a newly created file */
    public static final int MDBM_DEFAULT_HASH = MDBM_HASH_FNV;

    public static final int MDBM_MAX_HASH = 10;

    /**
     * Page size range supported. You don't want to make it smaller and you can't make it bigger - the offsets in the
     * page are 16 bits.
     */
    // FIXME: change this to 64 for mdbm-4.3.23
    public static final int MDBM_PAGE_ALIGN = 0; // 64;
    public static final int MDBM_MINPAGE = 128;
    public static final int MDBM_MAXPAGE = ((16 * 1024 * 1024) - MDBM_PAGE_ALIGN);
    public static final int MDBM_PAGESIZ = 4096;
    public static final int MDBM_MIN_PSHIFT = 7;

    /**
     * Magic number which is also a version number. If the database format changes then we need another magic number.
     */
    public static final int _MDBM_MAGIC = 0x01023962;
    public static final int _MDBM_MAGIC_NEW = 0x01023963;

}
