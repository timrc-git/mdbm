/* Copyright 2017 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */

package com.yahoo.db.mdbm;

import com.yahoo.db.mdbm.exceptions.MdbmException;

/**
 * This class takes an MdbmInterface as it's constructor, and locks the mdbm. The use case is for locking during
 * iteration via try-with resources.
 * 
 * Credit to bbhopesh for noticing this was missing.
 *
 */
public class MdbmLocker implements AutoCloseable {
    private MdbmInterface m;

    public MdbmLocker(MdbmInterface m) throws MdbmException {
        this.m = m;
        m.lock();
    }

    @Override
    public void close() throws MdbmException {
        if (null != m) {
            m.unlock();
            m = null;
        }
    }
}
