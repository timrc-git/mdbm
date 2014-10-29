/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmUnlockFailedException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmUnlockFailedException() {
        super();
    }

    public MdbmUnlockFailedException(String message) {
        super(message);
    }

}
