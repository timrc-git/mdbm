/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.internal.Deallocator.Dealloc;

/**
 * This class takes advanatage of the Cleaner API inside the GC to do native memory collection without a finalizer. The
 * Cleaner is a special type of PhatomReference and when the GC see's it during reference processing, it will call the
 * runnable method. This allows us to call a special JNI Free function that is implemented in Destructor, which only
 * takes a long which is actually a C pointrer.
 * 
 * This also deals with ensuring that we don't double free if the resource has been closed.
 * 
 * @author areese
 * 
 */
public abstract class DeallocatingClosedBase extends ClosedBaseChecked {
    protected volatile long pointer = 0L;
    protected volatile Deallocator deallocator;
    @SuppressWarnings("restriction")
    protected volatile sun.misc.Cleaner cleaner;

    @SuppressWarnings("restriction")
    protected DeallocatingClosedBase(long pointer, Dealloc destructor) {
        super(false, true);
        this.pointer = pointer;
        if (null != destructor) {
            this.deallocator = new Deallocator(pointer, destructor);
            this.cleaner = sun.misc.Cleaner.create(this, deallocator);
        } else {
            this.deallocator = null;
            this.cleaner = null;
        }
    }

    @Override
    protected boolean release() {
        if (0 != pointer) {
            release(pointer);

            // pointer is free set it to 0.
            this.pointer = 0L;

            if (null != deallocator) {
                // ensure the deallocator doesn't think it owns the pointer.
                this.deallocator.swigCPtr = 0L;

                // releases the deallocator
                this.deallocator = null;
            }
            // release the cleaner
            this.cleaner = null;
        }
        return true;
    }

    protected abstract void release(final long pointer);
}
