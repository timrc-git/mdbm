/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmLoadException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmLoadException() {
        super();
    }

    public MdbmLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmLoadException(String message) {
        super(message);
    }

    public MdbmLoadException(Throwable cause) {
        super(cause);
    }

}
