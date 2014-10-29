/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmNoMemoryException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmNoMemoryException() {
        super();
    }

    public MdbmNoMemoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmNoMemoryException(String message) {
        super(message);
    }

    public MdbmNoMemoryException(Throwable cause) {
        super(cause);
    }

}
