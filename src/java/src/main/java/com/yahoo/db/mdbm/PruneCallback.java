/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

/**
 * Callback adaptor for mdbm_prune function.
 * 
 * prune takes 3 arguments: key, value, and a userObject. The callback is always called with the result from
 * getUserObject.
 * 
 * Currently unsupported.
 * 
 * @author areese
 * 
 */
public interface PruneCallback {
    /**
     * If the prune function returns 1, the item is deleted. If the prune function returns 0, the item is retained.
     * 
     * @param key
     * @param value
     * @return 1 to delete, 0 to retain
     */
    int prune(MdbmDatum key, MdbmDatum value);

    Object getUserObject();
}
