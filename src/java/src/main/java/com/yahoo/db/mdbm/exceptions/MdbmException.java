/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.exceptions;

public class MdbmException extends Exception {
    private static final long serialVersionUID = 1L;
    private String path = "";
    private String info = "";

    public MdbmException() {
        super();
    }

    public MdbmException(String message) {
        super(message);
    }

    public MdbmException(Throwable cause) {
        super(cause);
    }

    public MdbmException(String message, Throwable cause) {
        super(message, cause);
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public void setPath(String file) {
        this.path = file;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + " for path '" + this.path + "' with extra info " + this.info;
    }
}
