package ru.ibs.dtm.query.execution.core.dao.exception.delta;

public class DeltaException extends RuntimeException {

    public DeltaException(String message) {
        super(message);
    }

    public DeltaException(String message, Throwable cause) {
        super(message + ": " + cause.getMessage(), cause);
    }
}
