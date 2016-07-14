/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.db.mdbm.Constants;
import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmIterator;
import com.yahoo.db.mdbm.MdbmKvPair;
import com.yahoo.db.mdbm.MdbmProvider;
import com.yahoo.db.mdbm.Open;
import com.yahoo.db.mdbm.Store;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmNoEntryException;
import com.yahoo.db.mdbm.exceptions.MdbmDeleteException;

public abstract class TestSimpleMdbm {
    public static final String testMdbmV3Path = "test/resources/testv3.mdbm";
    public static final String createMdbmV3Path = "target/create_testv3.mdbm";
    public static final String fetchMdbmV3Path = "target/fetch_testv3.mdbm";
    public static final String fetchMdbmV3PathSingleArch = "target/fetch_testv3_single_arch.mdbm";
    public static final String iteratorMdbmV3PathA = "target/iterator_testv3.mdbm";
    public static final String emptyMdbmV3Path = "target/empty_testv3.mdbm";
    public static final String deleteMdbmV3Path = "target/delete_testv3.mdbm";

    @Test(dataProvider = "createMdbms")
    public static MdbmInterface testCreate(String path, int flags, boolean close) throws MdbmException {
        MdbmInterface mdbm = null;

        String key = "abc";
        String value = "def";
        try {
            File f = new File(path);
            f.delete();
            Assert.assertFalse(f.exists());

            mdbm = MdbmProvider.open(path, flags, 0755, 0, 0);
            Assert.assertFalse(mdbm.isClosed());

            mdbm.storeString(key, value, Store.MDBM_REPLACE);
            String ret = mdbm.fetchString(key);
            Assert.assertEquals(ret, value);
            try {
                ret = mdbm.fetchString("nothere");
            } catch (MdbmNoEntryException e) {
            }

            return mdbm;
        } finally {
            if (close && null != mdbm)
                mdbm.close();
        }

    }

    @Test(dataProvider = "versions")
    public void testVersion(String path) throws MdbmException {
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(path, Open.MDBM_O_RDONLY, 0755, 0, 0);
        } catch (java.lang.AssertionError ae) {
            dumpStats(path);
            throw ae;
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test
    public void testEmptyFirst() throws MdbmException {
        MdbmInterface mdbm = null;
        MdbmIterator iter = null;
        try {
            mdbm = MdbmProvider.open(testMdbmV3Path, Open.MDBM_O_RDONLY, 0755, 0, 0);
            iter = mdbm.iterator();

            Assert.assertNotNull(iter);

            MdbmKvPair kv = mdbm.first(iter);

            Assert.assertNull(kv);
            kv = mdbm.next(iter);

            Assert.assertNull(kv);
        } finally {
            if (null != mdbm)
                mdbm.close();
            if (null != iter)
                iter.close();
        }
    }

    @Test(dataProvider = "iterateMdbms")
    public static void testIterate(String path, int flags) throws MdbmException {
        MdbmInterface mdbm = null;

        try {
            mdbm = testCreate(path, flags, false);
            int max = 10;
            for (int i = 0; i < max; i++) {
                String s = Integer.toString(i);
                mdbm.storeString(s, s, 0);
            }
            for (int i = 0; i < max; i++) {
                String k = Integer.toString(i);
                String v = mdbm.fetchString(k);
                Assert.assertNotNull(v);
            }
            String k = Integer.toString(max);
            String v = mdbm.fetchString(k);
            Assert.assertNull(v);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test
    public void testFetchString() throws MdbmException, UnsupportedEncodingException {
        String key = "testkey";
        MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm =
                            MdbmProvider.open(fetchMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                                            | Open.MDBM_O_CREAT, 0755, 0, 0);
            mdbm.plock(datum, 0);
            mdbm.storeString(key, key, Constants.MDBM_REPLACE);
            mdbm.punlock(datum, 0);
            Assert.assertEquals(mdbm.fetchString(key), key);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test
    public void testFetch() throws MdbmException, UnsupportedEncodingException {
        String key = "testkey";
        MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm =
                            MdbmProvider.open(fetchMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                                            | Open.MDBM_O_CREAT, 0755, 0, 0);
            mdbm.plock(datum, 0);
            mdbm.store(datum, datum, Constants.MDBM_REPLACE, mdbm.iterator());
            mdbm.punlock(datum, 0);
            System.err.println("Calling fetch");
            MdbmDatum data = mdbm.fetch(datum, mdbm.iterator());
            Assert.assertNotNull(data);
            Assert.assertNotNull(data.getData());
            Assert.assertEquals(new String(data.getData()), key);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test(expectedExceptions = { MdbmNoEntryException.class })
    public void testFetchNoEntry() throws MdbmException, UnsupportedEncodingException {
        String key = "nothere";
        MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(fetchMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                                            | Open.MDBM_O_CREAT, 0755, 0, 0);
            MdbmDatum data = mdbm.fetch(datum, mdbm.iterator());
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test
    public void testFetchWithoutIterator() throws MdbmException, UnsupportedEncodingException {
        String key = "testkey";
        MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm =
                            MdbmProvider.open(fetchMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                                            | Open.MDBM_O_CREAT, 0755, 0, 0);
            mdbm.plock(datum, 0);
            mdbm.store(datum, datum, Constants.MDBM_REPLACE, mdbm.iterator());
            mdbm.punlock(datum, 0);
            System.err.println("Calling fetch");
            MdbmDatum data = mdbm.fetch(datum);
            Assert.assertNotNull(data);
            Assert.assertNotNull(data.getData());
            Assert.assertEquals(new String(data.getData()), key);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }


    @Test
    public void testDeleteString() throws MdbmException, UnsupportedEncodingException {
        String key = "key_for_delete_string";
        String value = "value_for_delete_string";
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);

            mdbm.storeString(key, value, Store.MDBM_REPLACE);
            String ret = mdbm.fetchString(key);
            Assert.assertEquals(ret, value);

            mdbm.deleteString(key);
            String ret2 = mdbm.fetchString(key);
            Assert.assertNull(ret2);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test
    public void testDelete() throws MdbmException, UnsupportedEncodingException {
        String key = "key_for_delete";
        String value = "value_for_delete";
        MdbmDatum kDatum = new MdbmDatum(key.getBytes("Utf-8"));
        MdbmDatum vDatum = new MdbmDatum(value.getBytes("Utf-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);

            mdbm.store(kDatum, vDatum, Store.MDBM_REPLACE, mdbm.iterator());
            MdbmDatum ret = mdbm.fetch(kDatum);
            Assert.assertNotNull(ret);
            Assert.assertNotNull(ret.getData());
            Assert.assertEquals(new String(ret.getData()), value);

            mdbm.delete(kDatum);
            try {
                mdbm.fetch(kDatum); // throws MdbmNoEntryException
                Assert.fail();
            } catch (MdbmNoEntryException e) {
                // expected. ok.
            }
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test
    public void testDeleteIterator() throws MdbmException, UnsupportedEncodingException {
        String key = "key_for_delete_iterator";
        String value = "value_for_delete_iterator";
        MdbmDatum kDatum = new MdbmDatum(key.getBytes("Utf-8"));
        MdbmDatum vDatum = new MdbmDatum(value.getBytes("Utf-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);

            mdbm.store(kDatum, vDatum, Store.MDBM_REPLACE, mdbm.iterator());
            MdbmIterator iter = mdbm.iterator();
            MdbmDatum ret = mdbm.fetch(kDatum, iter);
            Assert.assertNotNull(ret);
            Assert.assertNotNull(ret.getData());
            Assert.assertEquals(new String(ret.getData()), value);

            mdbm.delete(iter);
            try {
                mdbm.fetch(kDatum); // throws MdbmNoEntryException
                Assert.fail();
            } catch (MdbmNoEntryException e) {
                // expected. ok.
            }
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test(expectedExceptions = { MdbmNoEntryException.class })
    public void testDeleteStringNoEntry() throws MdbmException, UnsupportedEncodingException {
        String key = "nothere";
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);
            mdbm.deleteString(key);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test(expectedExceptions = { MdbmNoEntryException.class })
    public void testDeleteNoEntry() throws MdbmException, UnsupportedEncodingException {
        String key = "nothere";
        MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);
            mdbm.delete(datum);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test(expectedExceptions = { MdbmNoEntryException.class })
    public void testDeleteIteratorNoEntry() throws MdbmException, UnsupportedEncodingException {
        String key = "key_for_delete_iterator_noentry";
        String value = "value_for_delete_iterator_noentry";
        MdbmDatum kDatum = new MdbmDatum(key.getBytes("Utf-8"));
        MdbmDatum vDatum = new MdbmDatum(value.getBytes("Utf-8"));
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);

            mdbm.store(kDatum, vDatum, Store.MDBM_REPLACE, mdbm.iterator());
            MdbmIterator iter = mdbm.iterator();
            MdbmDatum ret = mdbm.fetch(kDatum, iter);
            Assert.assertNotNull(ret);
            Assert.assertNotNull(ret.getData());
            Assert.assertEquals(new String(ret.getData()), value);

            mdbm.delete(iter); // success
            mdbm.delete(iter); // throws MdbmNoEntryException

            /*
            MdbmIterator iter = mdbm.iterator();
            mdbm.delete(iter);
            */
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test(expectedExceptions = { MdbmDeleteException.class })
    public void testDeleteInvalid() throws MdbmException, UnsupportedEncodingException {
        MdbmDatum datum = new MdbmDatum(0);
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);
            mdbm.delete(datum);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    @Test(expectedExceptions = { MdbmDeleteException.class })
    public void testDeleteIteratorInvalid() throws MdbmException, UnsupportedEncodingException {
        MdbmInterface mdbm = null;
        try {
            mdbm = MdbmProvider.open(deleteMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                    | Open.MDBM_O_CREAT, 0755, 0, 0);

            // MdbmIterator iter = mdbm.iterator();
            MdbmIterator iter = null;
            mdbm.delete(iter);
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    public static void dumpStats(String file) {
        try {
            // ProcessBuilder pb = new ProcessBuilder("mdbm_stat",
            // "-H", file);
            // pb.redirectErrorStream(true);
            // Process p = pb.start();
            Process p = Runtime.getRuntime().exec("mdbm_stat" + " -H " + file);

            Thread.sleep(1000);
            BufferedReader isr = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
            String line = null;
            while ((line = isr.readLine()) != null) {
                System.err.println(line);
            }

            p.exitValue();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
