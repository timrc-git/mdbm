/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.yahoo.db.mdbm.internal.Deallocator.Dealloc;

public class TestDeallocator {

    @SuppressWarnings("boxing")
    @DataProvider
    public Object[][] destructors() {
        return new Object[][] { //
        //
                        {0L, false}, //
                        {0L, true}, //
                        {1L, false}, //
                        {1L, true}, //
        };
    }

    @Test(dataProvider = "destructors")
    public void test1(long pointer, boolean own) {
        Dealloc destructor = Mockito.mock(Dealloc.class);
        Deallocator dealloc = new Deallocator(pointer, destructor);

        internalTest(destructor, dealloc, pointer, true);
    }

    @Test(dataProvider = "destructors")
    public void test2(long pointer, boolean own) {
        Dealloc destructor = Mockito.mock(Dealloc.class);
        Deallocator dealloc = new Deallocator(pointer, own, destructor);
        internalTest(destructor, dealloc, pointer, own);
    }

    private void internalTest(Dealloc destructor, Deallocator dealloc, long pointer, boolean own) {
        dealloc.run();

        if (0 != pointer && own) {
            Mockito.verify(destructor).delete(Matchers.eq(pointer));
        } else {
            Mockito.verify(destructor, Mockito.never()).delete(Matchers.anyLong());
        }
    }

    private static final class TestDeallocatingClosedBase extends DeallocatingClosedBase {

        private boolean fail;

        protected TestDeallocatingClosedBase(long pointer, Dealloc destructor, boolean fail) {
            super(pointer, destructor);
            this.fail = fail;
        }

        @Override
        protected void release(long pointer) {
            if (this.fail) {
                Assert.fail();
            }
        }
    }


    @Test(dataProvider = "destructors")
    public void test3(long pointer, boolean own) {
        Dealloc destructor = Mockito.mock(Dealloc.class);
        DeallocatingClosedBase d = new TestDeallocatingClosedBase(pointer, destructor, 0 == pointer);

        d.close();

    }

}
