/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmIllegalOperationException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmIllegalOperationException() {
        super();
    }

    public MdbmIllegalOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmIllegalOperationException(String message) {
        super(message);
    }

    public MdbmIllegalOperationException(Throwable cause) {
        super(cause);
    }

}
