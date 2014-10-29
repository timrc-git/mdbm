/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

public class Deallocator implements Runnable {

    public static interface Dealloc {
        void delete(long swigCPtr);
    }

    public long swigCPtr;
    public boolean swigCMemOwn;
    public final Dealloc destructor;

    public Deallocator(long swigCPtr, boolean swigCMemOwn, Dealloc destructor) {
        this.swigCPtr = swigCPtr;
        this.swigCMemOwn = swigCMemOwn;
        this.destructor = destructor;
        // System.err.println("Finalizer created: " + swigCPtr + " " + swigCMemOwn);
    }

    public Deallocator(long pointer, Dealloc destructor) {
        this(pointer, true, destructor);
    }

    @Override
    public void run() {
        // System.err.println("Finalizer run: " + swigCPtr + " " + swigCMemOwn);
        if (swigCPtr != 0) {
            if (swigCMemOwn) {
                swigCMemOwn = false;
                destructor.delete(swigCPtr);
                // System.err.println("delete done: " + swigCPtr + " " + swigCMemOwn);
            }
            swigCPtr = 0;
        }
    }

}
