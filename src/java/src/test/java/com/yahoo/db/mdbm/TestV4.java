/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.yahoo.db.mdbm.Constants;
import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmProvider;
import com.yahoo.db.mdbm.Open;
import com.yahoo.db.mdbm.exceptions.MdbmException;

@SuppressWarnings("boxing")
public class TestV4 extends TestSimpleMdbm {
    @DataProvider
    public Object[][] createMdbms() {
        return new Object[][] {new Object[] {createMdbmV3Path,
                        Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR | Open.MDBM_O_CREAT, true},};
    }

    @DataProvider
    public Object[][] emptyMdbms() {
        return new Object[][] {new Object[] {emptyMdbmV3Path,
                Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR | Open.MDBM_O_CREAT,},};
    }

    @DataProvider
    public Object[][] iterateMdbms() {
        return new Object[][] {new Object[] {iteratorMdbmV3PathA,
                        Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR | Open.MDBM_O_CREAT,},};
    }

    @DataProvider
    public Object[][] versions() {
        return new Object[][] {new Object[] {testMdbmV3Path},};
    }

    @Test
    public static void testFetchNativeSingleArchSpawnedNoArch() throws MdbmException, IOException {
        int nativeFlags = Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR | Open.MDBM_O_CREAT;
        int spawnedFlags = nativeFlags | Open.MDBM_SINGLE_ARCH;
        testFetchNative(fetchMdbmV3PathSingleArch, nativeFlags, spawnedFlags);
    }

    public static void testFetchNative(String mdbmPath, int nativeFlags, int spawnedFlags) throws MdbmException,
                    IOException {
        String key = new String("testkey");
        MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
        MdbmInterface mdbm = null;
        System.err.println(mdbmPath + " with spawned flags: " + Integer.toHexString(spawnedFlags)
                        + " and native flags: " + Integer.toHexString(nativeFlags));

        openMdbmFrom32bit(mdbmPath, spawnedFlags);
        System.err.println("opened: " + mdbmPath + " with spawned flags: " + Integer.toHexString(spawnedFlags));

        try {
            mdbm = MdbmProvider.open(mdbmPath, nativeFlags, 0755, 0, 0);
            System.err.println(mdbmPath + " with spawned flags: " + Integer.toHexString(spawnedFlags)
                            + " and native flags: " + Integer.toHexString(nativeFlags));
            System.err.println("opened: " + mdbmPath + " with native flags: " + Integer.toHexString(nativeFlags));
            mdbm.plock(datum, 0);
            mdbm.store(datum, datum, Constants.MDBM_REPLACE, mdbm.iterator());
            mdbm.punlock(datum, 0);
            System.err.println("Calling fetch");
            MdbmDatum data = mdbm.fetch(datum, mdbm.iterator());
            Assert.assertNotNull(data);
            Assert.assertNotNull(data.getData());
            Assert.assertEquals(new String(data.getData()), key);
            System.err.println("fetch done");
        } finally {
            if (null != mdbm)
                mdbm.close();
        }
    }

    public static void main(String[] args) throws MdbmException, IOException {
        testFetchNativeSingleArchSpawnedNoArch();
    }

    @SuppressWarnings("resource")
    private static void openMdbmFrom32bit(String fetchmdbmv3path, int spawnedFlags) throws IOException {
        String args[] =
                        new String[] {
                                        "java",
                                        "-d32",
                                        "-Djava.library.path=target/native/i686-linux-gcc",
                                        "-cp",
                                        "target/classes/:~/.m2/repositorycom/beust/jcommander/1.12/jcommander-1.12.jar:~/.m2/repositoryjunit/junit/3.8.1/junit-3.8.1.jar:~/.m2/repositorylog4j/log4j/1.2.14/log4j-1.2.14.jar:~/.m2/repositoryorg/beanshell/bsh/2.0b4/bsh-2.0b4.jar:~/.m2/repositoryorg/testng/testng/6.3.1/testng-6.3.1.jar:~/.m2/repositoryorg/yaml/snakeyaml/1.6/snakeyaml-1.6.jar",
                                        "yjava.db.mdbm.TestOpenMdbm", fetchmdbmv3path, Integer.toString(spawnedFlags),
                                        Integer.toString(5),};

        Process process = new ProcessBuilder(args).start();
        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;

        System.out.printf("Output of running %s is:", Arrays.toString(args));

        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

    }
}
