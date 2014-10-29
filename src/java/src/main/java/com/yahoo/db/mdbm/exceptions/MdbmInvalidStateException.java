/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmInvalidStateException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmInvalidStateException() {
        super();
    }

    public MdbmInvalidStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmInvalidStateException(String message) {
        super(message);
    }

    public MdbmInvalidStateException(Throwable cause) {
        super(cause);
    }

}
