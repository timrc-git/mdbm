/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.nio.charset.Charset;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMdbmKvPair {
    @Test
    public void testDefault() {
        MdbmKvPair kvPair = new MdbmKvPair();
        Assert.assertNull(kvPair.getKey());
        Assert.assertNull(kvPair.getValue());
    }

    @Test
    public void testExplicit() {
        MdbmDatum key = new MdbmDatum("key".getBytes(Charset.forName("UTF-8")));
        MdbmDatum value = new MdbmDatum("value".getBytes(Charset.forName("UTF-8")));

        MdbmKvPair kvPair = new MdbmKvPair(key, value);
        MdbmKvPair kvPair2 = new MdbmKvPair();
        MdbmKvPair kvPair3 = new MdbmKvPair(key, key);
        MdbmKvPair kvPair4 = new MdbmKvPair(key, null);
        MdbmKvPair kvPair5 = new MdbmKvPair(null, value);

        Assert.assertEquals(kvPair.getKey(), key);
        Assert.assertEquals(kvPair.getValue(), value);

        Assert.assertSame(kvPair.getKey(), key);
        Assert.assertSame(kvPair.getValue(), value);

        Assert.assertTrue(kvPair.equals(kvPair));
        Assert.assertFalse(kvPair.equals(null));
        Assert.assertFalse(kvPair.equals(kvPair2));
        Assert.assertFalse(kvPair.equals(kvPair3));
        Assert.assertFalse(kvPair.equals(kvPair4));
        Assert.assertFalse(kvPair.equals(kvPair5));
        Assert.assertFalse(kvPair.equals(new Double("1")));

        Assert.assertTrue(kvPair2.equals(kvPair2));
        Assert.assertFalse(kvPair2.equals(null));
        Assert.assertFalse(kvPair2.equals(kvPair));
        Assert.assertFalse(kvPair2.equals(kvPair3));
        Assert.assertFalse(kvPair2.equals(kvPair4));
        Assert.assertFalse(kvPair2.equals(kvPair5));

        kvPair.toString();
        kvPair2.toString();
        kvPair3.toString();
        kvPair4.toString();
    }
}
