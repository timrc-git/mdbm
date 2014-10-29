/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

/**
 * See: for details. http://docs.corp.yahoo.com/svn/yahoo/stack/core/mdbm/docs/mdbm_shake.html
 * 
 * This interface wraps shake v3 only.
 * 
 * Currently unsupported.
 * 
 * @author areese
 * 
 */
public interface ShakeCallback {
    /**
     * @param key
     * @param value
     * @return 1 to delete, 0 to retain
     */
    int shake(MdbmDatum key, MdbmDatum value);

    Object getUserObject();
}
