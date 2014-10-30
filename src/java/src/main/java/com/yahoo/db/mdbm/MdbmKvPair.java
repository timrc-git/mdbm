/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

public class MdbmKvPair {
    private MdbmDatum key;
    private MdbmDatum value;

    public MdbmKvPair() {
        this(null, null);
    }

    public MdbmKvPair(MdbmDatum key, MdbmDatum value) {
        this.key = key;
        this.value = value;
    }

    public MdbmDatum getKey() {
        return key;
    }

    public void setKey(MdbmDatum key) {
        this.key = key;
    }

    public MdbmDatum getValue() {
        return value;
    }

    public void setValue(MdbmDatum value) {
        this.value = value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
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
        MdbmKvPair other = (MdbmKvPair) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "key :'" + ((null == key) ? "null" : key.toString()) + "' value: '"
                        + ((null == value) ? "null" : value.toString()) + "'";
    }
}
