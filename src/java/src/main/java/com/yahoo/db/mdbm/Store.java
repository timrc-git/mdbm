/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

public class Store {
    /*
     * flags to mdbm_store
     */
    public static final int MDBM_INSERT = 0;
    public static final int MDBM_REPLACE = 1;
    public static final int MDBM_INSERT_DUP = 2;
    public static final int MDBM_MODIFY = 3;
    public static final int MDBM_STORE_MASK = 0x3;

    public static final int MDBM_RESERVE = 0x100;

    /** mark entry as clean */
    public static final int MDBM_CLEAN = 0x200;

    /**
     * do not operate on the backing store; use cache only
     */
    public static final int MDBM_CACHE_ONLY = 0x400;

    /**
     * update cache if key exists; insert if does not exist
     */
    public static final int MDBM_CACHE_REPLACE = 0;

    /**
     * update cache if key exists; do not insert if does not
     */
    public static final int MDBM_CACHE_MODIFY = 0x1000;

    /**
     * returned if store succeeds
     */
    public static final int MDBM_STORE_SUCCESS = 0;

    /**
     * returned if MDBM_INSERT used and key exists
     */
    public static final int MDBM_STORE_ENTRY_EXISTS = 1;

}
