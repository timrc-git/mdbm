/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import org.testng.annotations.Test;

import com.yahoo.db.mdbm.exceptions.InvalidMdbmParametersException;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmDeleteException;
import com.yahoo.db.mdbm.exceptions.MdbmFetchException;
import com.yahoo.db.mdbm.exceptions.MdbmIllegalOperationException;
import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;
import com.yahoo.db.mdbm.exceptions.MdbmLoadException;
import com.yahoo.db.mdbm.exceptions.MdbmLockFailedException;
import com.yahoo.db.mdbm.exceptions.MdbmNoEntryException;
import com.yahoo.db.mdbm.exceptions.MdbmNoMemoryException;
import com.yahoo.db.mdbm.exceptions.MdbmStoreException;
import com.yahoo.db.mdbm.exceptions.MdbmUnableToLockException;
import com.yahoo.db.mdbm.exceptions.MdbmUnlockFailedException;
import com.yahoo.db.mdbm.exceptions.SharedLockViolationException;

public class TestExceptions {
    @Test
    public void testExceptions() {
        Exception e = new Exception();

        e = new MdbmException();
        e = new MdbmException("message");
        e = new MdbmException(e);
        e = new MdbmException("message", e);

        new MdbmException().setInfo("");
        new MdbmException().setPath("");

        e = new InvalidMdbmParametersException();
        e = new InvalidMdbmParametersException("message");
        e = new InvalidMdbmParametersException(e);
        e = new InvalidMdbmParametersException("message", e);

        e = new MdbmIllegalOperationException();
        e = new MdbmIllegalOperationException("message");
        e = new MdbmIllegalOperationException(e);
        e = new MdbmIllegalOperationException("message", e);

        e = new MdbmInvalidStateException();
        e = new MdbmInvalidStateException("message");
        e = new MdbmInvalidStateException(e);
        e = new MdbmInvalidStateException("message", e);

        e = new MdbmLoadException();
        e = new MdbmLoadException("message");
        e = new MdbmLoadException(e);
        e = new MdbmLoadException("message", e);

        e = new MdbmLockFailedException();
        e = new MdbmLockFailedException("message");
        // e = new MdbmLockFailedException(e);
        // e = new MdbmLockFailedException("message", e);

        e = new MdbmNoEntryException();
        e = new MdbmNoEntryException("message");
        e = new MdbmNoEntryException(e);
        e = new MdbmNoEntryException("message", e);

        e = new MdbmNoMemoryException();
        e = new MdbmNoMemoryException("message");
        e = new MdbmNoMemoryException(e);
        e = new MdbmNoMemoryException("message", e);

        e = new SharedLockViolationException();
        e = new SharedLockViolationException("message");
        e = new SharedLockViolationException(e);
        e = new SharedLockViolationException("message", e);

        e = new MdbmDeleteException();
        e = new MdbmDeleteException("message");

        e = new MdbmFetchException();
        e = new MdbmFetchException("message");

        e = new MdbmStoreException();
        e = new MdbmStoreException("message");

        e = new MdbmUnableToLockException();
        e = new MdbmUnableToLockException("message");
        e = new MdbmUnableToLockException(e);
        e = new MdbmUnableToLockException("message", e);

        e = new MdbmUnlockFailedException();
        e = new MdbmUnlockFailedException("message");
    }
}
