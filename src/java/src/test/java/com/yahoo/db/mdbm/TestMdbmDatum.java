/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.io.File;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmProvider;
import com.yahoo.db.mdbm.Open;
import com.yahoo.db.mdbm.Store;
import com.yahoo.db.mdbm.exceptions.MdbmNoEntryException;

@SuppressWarnings("unused")
public class TestMdbmDatum {

    @Test
    public void testEmpty0() {
        MdbmDatum mdbmDatum = new MdbmDatum(0);
        mdbmDatum.toString();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyInvalid() {
        new MdbmDatum(-1);
    }

    @Test
    public void testEmpty2() {
        MdbmDatum mdbmDatum = new MdbmDatum((byte[]) null);
        mdbmDatum.toString();
    }

    @Test
    public void testEmpty3() {
        MdbmDatum mdbmDatum = new MdbmDatum(new byte[] {});
        mdbmDatum.toString();
    }

    @Test
    public void testBuffer() {
        byte[] buffer = new byte[] {1, 2, 3};
        MdbmDatum datum = new MdbmDatum(buffer);
        MdbmDatum datum2 = new MdbmDatum();

        Assert.assertSame(datum.getData(), buffer);
        Assert.assertEquals(datum.getSize(), buffer.length);
        Assert.assertTrue(datum.equals(datum));
        Assert.assertFalse(datum.equals(null));
        Assert.assertFalse(datum.equals(""));
        Assert.assertFalse(datum.equals(datum2));

        datum.toString();
        datum2.toString();
        datum.hashCode();
        datum2.hashCode();

        datum2.setData(buffer);
        Assert.assertTrue(datum.equals(datum2));
    }
}
