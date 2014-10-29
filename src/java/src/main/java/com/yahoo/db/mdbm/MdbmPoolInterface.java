/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.io.Closeable;

import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;

/**
 * This class maintains a fixed size pool of mdbm handles that can be used. This is the preferred way to use mdbm from
 * more than one thread.
 * 
 * @author areese
 * 
 */
public interface MdbmPoolInterface extends Closeable {

    /**
     * get an mdbm handle from the pool, blocking if required.
     * 
     * @return mdbm handle from the pool.
     * @throws MdbmInvalidStateException
     */
    MdbmInterface getMdbmHandle() throws MdbmInvalidStateException;

    /**
     * return an mdbm handle to the pool
     * 
     * @param mdbm handle to return
     * @throws MdbmInvalidStateException
     */
    void returnMdbmHandle(MdbmInterface mdbm) throws MdbmInvalidStateException;

    /** Non-wrapper functions **/

    /**
     * @return Returns the canonical path to the mdbm.
     */
    String getPath();

    /**
     * @return Has the mdbm been closed?
     */
    boolean isClosed();

    /**
     * Validate the internal state of the Mdbm Object
     * 
     * @throws MdbmInvalidStateException
     */
    void validate() throws MdbmInvalidStateException;

    @Override
    void close();

}
