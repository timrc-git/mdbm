/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.io.Closeable;

public interface MdbmIterator extends Closeable {
    /**
     * Not for public consumption.
     * 
     * @return an Opaque object for internal use.
     */
    public Object getIter();

    @Override
    public void close();
}
