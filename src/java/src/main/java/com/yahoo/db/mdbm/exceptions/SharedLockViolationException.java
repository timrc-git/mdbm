/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class SharedLockViolationException extends MdbmInvalidStateException {
    private static final long serialVersionUID = 1L;

    public SharedLockViolationException() {
        this("Mdbm was not opened with yjava.db.mdbm.Open.MDBM_RW_LOCKS enabled");
    }

    public SharedLockViolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SharedLockViolationException(String message) {
        super(message);
    }

    public SharedLockViolationException(Throwable cause) {
        super(cause);
    }

}
