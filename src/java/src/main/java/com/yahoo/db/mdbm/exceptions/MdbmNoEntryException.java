/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmNoEntryException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmNoEntryException() {
        super();
    }

    public MdbmNoEntryException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmNoEntryException(String message) {
        super(message);
    }

    public MdbmNoEntryException(Throwable cause) {
        super(cause);
    }

}
