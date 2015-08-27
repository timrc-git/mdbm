package com.yahoo.db.mdbm.internal;

import java.util.ArrayList;
import java.util.List;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmPoolInterface;
import com.yahoo.db.mdbm.MdbmProvider;
import com.yahoo.db.mdbm.Open;
import com.yahoo.db.mdbm.TestSimpleMdbm;
import com.yahoo.db.mdbm.exceptions.MdbmCreatePoolException;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmNoEntryException;
import com.yahoo.db.mdbm.internal.PooledMdbmHandle;

public class TestMdbmPool {
    @SuppressWarnings("boxing")
    @DataProvider
    public Object[][] invalidSizes() {
        return new Object[][] { {-1}, {0}};
    }

    @Test(dataProvider = "invalidSizes", expectedExceptions = MdbmCreatePoolException.class)
    public void testInvalidSize(int poolSize) throws MdbmException {
        System.err.println("start testInvalidSize");
        MdbmProvider.openPool(TestSimpleMdbm.fetchMdbmV3Path, Open.MDBM_CREATE_V3 | Open.MDBM_O_RDWR
                        | Open.MDBM_O_CREAT, 0755, 0, 0, poolSize);
    }

    @Test
    public void testPool() throws MdbmException {
        System.err.println("start testPool");

        final int size = 10;

        MdbmPoolInterface pool = null;
        try {
            pool =
                            MdbmProvider.openPool(TestSimpleMdbm.fetchMdbmV3Path, Open.MDBM_CREATE_V3
                                            | Open.MDBM_O_RDWR | Open.MDBM_O_CREAT, 0755, 0, 0, size);

            Assert.assertNotNull(pool);

            for (int i = 0; i < 4; i++) {
                List<MdbmInterface> handleList = new ArrayList<MdbmInterface>(size);

                // ensure we can get 10 handles
                for (int j = 0; j < size; j++) {
                    @SuppressWarnings("resource")
                    MdbmInterface mdbm = pool.getMdbmHandle();
                    Assert.assertNotNull(mdbm);
                    Assert.assertTrue(mdbm instanceof PooledMdbmHandle);

                    System.err.println("got handle " + i);
                    try {
                        mdbm.fetchString("nothere");
                    } catch (MdbmNoEntryException e) {
                    }

                    handleList.add(mdbm);
                }

                // then return them all.
                for (MdbmInterface mdbm : handleList) {
                    pool.returnMdbmHandle(mdbm);
                }
            }

        } finally {
            if (null != pool) {
                pool.close();
            }
        }
        System.err.println("end testPool");
    }

    @SuppressWarnings("resource")
    @Test
    public void testDontCloseFromPool() throws MdbmException {
        System.err.println("start testDontCloseFromPool");

        final int size = 10;
        MdbmPoolInterface origPool = null;
        NativeMdbmPoolImplementation pool = null;
        try {
            origPool =
                            MdbmProvider.openPool(TestSimpleMdbm.fetchMdbmV3Path, Open.MDBM_CREATE_V3
                                            | Open.MDBM_O_RDWR | Open.MDBM_O_CREAT, 0755, 0, 0, size);

            Assert.assertNotNull(origPool);

            pool = Mockito.spy((NativeMdbmPoolImplementation) origPool);

            Assert.assertNotNull(pool);

            for (int i = 0; i < 4; i++) {
                List<MdbmInterface> handleList = new ArrayList<MdbmInterface>(size);

                // ensure we can get 10 handles
                for (int j = 0; j < size; j++) {
                    MdbmInterface mdbm = pool.getMdbmHandle();
                    Assert.assertNotNull(mdbm);
                    handleList.add(mdbm);
                    mdbm.close();
                }

                // then return them all.
                for (MdbmInterface mdbm : handleList) {
                    pool.returnMdbmHandle(mdbm);
                }

                Mockito.verify(pool, Mockito.never()).internalReleaseHandle(Matchers.any(PooledMdbmHandle.class));
            }

        } finally {
            System.err.println("finally testDontCloseFromPool");
            if (null != pool) {
                pool.close();
            }
        }
        System.err.println("end testDontCloseFromPool");
    }
}
