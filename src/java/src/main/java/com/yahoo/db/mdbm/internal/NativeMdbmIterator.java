/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.MdbmIterator;

class NativeMdbmIterator extends DeallocatingClosedBase implements MdbmIterator {

    private static final Deallocator.Dealloc DESTRUCTOR = new Deallocator.Dealloc() {
        @Override
        public void delete(long pointer) {
            if (pointer != 0) {
                NativeMdbmAccess.freeIter(pointer);
            }
        }
    };

    private NativeMdbmIterator(final long pointer) {
        super(pointer, DESTRUCTOR);
        this.setOpen();
    }

    /**
     * This needs to return the internal type so we can use it.
     */
    @Override
    public Object getIter() {
        return Long.valueOf(this.pointer);
    }

    @Override
    public void release(long pointer) {
        if (0 == pointer) {
            return;
        }

        NativeMdbmAccess.freeIter(pointer);
        pointer = 0L;
    }

    long getPointer() {
        return pointer;
    }
}
