/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestClosedBaseChecked {
    private static final class TestClosed extends ClosedBaseChecked {
        @Override
        protected boolean release() {
            return true;
        }
    }

    private static final class TestDeallocating extends DeallocatingClosedBase {
        protected TestDeallocating() {
            super(0L, null);
        }

        @Override
        protected boolean release() {
            return true;
        }

        @Override
        protected void release(long pointer) {}
    }


    @SuppressWarnings("resource")
    @DataProvider
    public Object[][] closedBase() {
        return new Object[][] { {new TestClosed()}, {new TestDeallocating()}};
    }

    @Test(dataProvider = "closedBase")
    public void testDefault(ClosedBaseChecked c) {
        c.close();
    }

    @Test(dataProvider = "closedBase", expectedExceptions = IllegalStateException.class)
    public void testDoubleClose(ClosedBaseChecked c) {
        c.close();
        c.close();
    }
}
