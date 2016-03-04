package com.yahoo.db.mdbm;

import static com.yahoo.db.mdbm.TestSimpleMdbm.testMdbmV3Path;

import java.io.File;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPool {

    @Test
    public static void testEmptyFirst() throws Exception {
        MdbmPoolInterface mdbmPool = null;
        try {
            String path = new File(testMdbmV3Path).getAbsolutePath();
            System.err.println("opening " + path);
            mdbmPool = MdbmProvider.openPool(path, Open.MDBM_O_RDONLY, 0755, 0, 0, 5);
            try (MdbmInterface handle = mdbmPool.getMdbmHandle()) {
                String key = "key1";
                MdbmDatum datum = new MdbmDatum(key.getBytes("UTF-8"));
                MdbmDatum value = handle.fetch(datum);
                Assert.assertNotNull(value);
                // returning the handle here and in the closeable to ensure we don't get the pool cannot be closed, -1 handles have been returned bug
                mdbmPool.returnMdbmHandle(handle);
            }

        } finally {
            if (null != mdbmPool) {
                mdbmPool.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        testEmptyFirst();
    }
}
