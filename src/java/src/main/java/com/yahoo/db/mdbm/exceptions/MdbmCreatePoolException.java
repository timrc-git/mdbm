package com.yahoo.db.mdbm.exceptions;

public class MdbmCreatePoolException extends MdbmException {
    private static final long serialVersionUID = 1L;

    public MdbmCreatePoolException() {
        super();
    }

    public MdbmCreatePoolException(String message, Throwable cause) {
        super(message, cause);
    }

    public MdbmCreatePoolException(String message) {
        super(message);
    }

    public MdbmCreatePoolException(Throwable cause) {
        super(cause);
    }

}
