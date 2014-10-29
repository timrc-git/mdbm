/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmUnableToLockException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmUnableToLockException() {
        super();
    }

    public MdbmUnableToLockException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmUnableToLockException(String message) {
        super(message);
    }

    public MdbmUnableToLockException(Throwable cause) {
        super(cause);
    }

}
