/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.UncloseableMdbmException;

/**
 * This class is used by the pool to make sure that mdbm's it returns can't be closed out from under it.
 * 
 * @author areese
 * 
 */
class UncloseableMdbm extends NativeMdbmImplementation {

    protected UncloseableMdbm(final long pointer, String path, int flags) throws MdbmException {
        // we can't have someone free'ing our pointer out from under us/
        super(pointer, path, flags, false);
    }

    @Override
    public void release(long pointer) {
        // invalid
        throw new RuntimeException(new UncloseableMdbmException());
    }

    @Override
    public void closeFd() throws MdbmException {
        // invalid
        throw new UncloseableMdbmException();
    }

    @Override
    protected boolean release() {
        return true;
    }

    @Override
    public synchronized void close() {
        // invalid
        throw new RuntimeException(new UncloseableMdbmException());
    }
}
