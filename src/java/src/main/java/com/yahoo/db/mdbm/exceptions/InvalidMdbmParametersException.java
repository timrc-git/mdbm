/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class InvalidMdbmParametersException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public InvalidMdbmParametersException() {
        super();
    }

    public InvalidMdbmParametersException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMdbmParametersException(String message) {
        super(message);
    }

    public InvalidMdbmParametersException(Throwable cause) {
        super(cause);
    }

}
