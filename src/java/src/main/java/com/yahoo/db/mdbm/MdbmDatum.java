/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.util.Arrays;

/**
 * Class for wrapping an mdbm datum structure.
 * 
 * Datums are not thread safe.
 * 
 * @author areese
 * 
 */
// ignore that we allow direct access to the buffer we wrap.
@SuppressWarnings({"PMD. EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class MdbmDatum {
    protected byte[] buffer;

    public MdbmDatum() {
        this(null);
    }

    public MdbmDatum(final int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Cannot create an MdbmDatum of size < 0");
        }
        this.buffer = new byte[size];
    }

    public MdbmDatum(final byte[] array) {
        this.buffer = array;
    }

    public int getSize() {
        return buffer.length;
    }

    public byte[] getData() {
        return this.buffer;
    }

    public void setData(final byte[] data) {
        this.buffer = data;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(buffer);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MdbmDatum other = (MdbmDatum) obj;
        if (!Arrays.equals(buffer, other.buffer)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Arrays.toString(buffer);
    }
}
