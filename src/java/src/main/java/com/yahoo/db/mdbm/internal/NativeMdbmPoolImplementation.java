/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmPoolInterface;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;
import com.yahoo.db.mdbm.exceptions.MdbmPoolAcquireHandleFailedException;

/**
 * Wrapper around ccmp
 * 
 * @author areese
 * 
 */
class NativeMdbmPoolImplementation extends DeallocatingClosedBase implements MdbmPoolInterface {

    private static final Deallocator.Dealloc DESTRUCTOR = new Deallocator.Dealloc() {
        @Override
        public void delete(long pointer) {
            if (pointer != 0) {
                NativeMdbmAccess.destroy_pool(pointer);
            }
        }
    };

    private final MdbmInterface parentMdbmHandle;

    // for validating that we have returned all of the handles, that way we can
    // throw a RuntimeException on close.
    private final int totalSize;
    private final AtomicInteger handlesInFlight = new AtomicInteger(0);

    private NativeMdbmPoolImplementation(MdbmInterface mdbm, final long pointer, final int totalSize)
                    throws MdbmException {
        super(pointer, DESTRUCTOR);

        this.parentMdbmHandle = mdbm;
        this.setOpen();
        this.totalSize = totalSize;
    }

    @Override
    public void release(long pointer) {
        if (0 == pointer) {
            return;
        }

        // System.err.println("release at " + handlesInFlight.intValue() + " of " + totalSize);

        if (0 != handlesInFlight.intValue()) {
            throw new RuntimeException(new MdbmPoolAcquireHandleFailedException(
                            "unabled to destroy pool, currently have " + handlesInFlight.intValue()
                                            + " handles that haven't been returned to the pool out of " + totalSize));
        }

        NativeMdbmAccess.destroy_pool(pointer);
    }

    long getPointer() {
        return pointer;
    }

    @Override
    public MdbmInterface getMdbmHandle() throws MdbmInvalidStateException {
        validate();

        MdbmInterface mdbm = NativeMdbmAccess.acquire_handle(this);
        if (!(mdbm instanceof PooledMdbmHandle)) {
            throw new MdbmInvalidStateException("got a non PooledMdbmHandle  from the pool"
                            + mdbm.getClass().getCanonicalName());
        }

        ((PooledMdbmHandle) mdbm).setCheckedOut(true);
        handlesInFlight.incrementAndGet();
        // System.err.println("getMdbmHandle at " + handlesInFlight.get() + " of " + totalSize);
        return mdbm;
    }

    @Override
    public void returnMdbmHandle(MdbmInterface mdbm) throws MdbmInvalidStateException {
        validate();
        if (null == mdbm) {
            throw new NullPointerException();
        }

        if (!(mdbm instanceof PooledMdbmHandle)) {
            throw new MdbmInvalidStateException("can only return instances of PooledMdbmHandle to a pool, got "
                            + mdbm.getClass().getCanonicalName());
        }

        if (!mdbm.isClosed()) {
            internalReleaseHandle((PooledMdbmHandle) mdbm);
        }
    }

    void internalReleaseHandle(PooledMdbmHandle mdbm) {
        if (mdbm.isClosed() || !mdbm.isCheckedOut()) {
            return;
        }

        handlesInFlight.decrementAndGet();
        mdbm.setCheckedOut(false);
        // try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw);) {
        // new Exception().printStackTrace(pw);
        // System.err.println("internalReleaseHandle at " + handlesInFlight.get() + " of " + totalSize + " at "
        // + sw.toString());
        // } catch (IOException e) {
        // }
        NativeMdbmAccess.release_handle(this, mdbm);
    }

    @Override
    public String getPath() {
        return parentMdbmHandle.getPath();
    }

    @Override
    public void validate() throws MdbmInvalidStateException {
        boolean closed = isClosed();
        if (0 == pointer || closed) {
            throw new MdbmInvalidStateException("Mdbm is closed or not open: " + pointer + " closed: " + closed);
        }
    }

}
