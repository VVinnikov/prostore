package ru.ibs.dtm.query.execution.core.dao.exception;

public class DatamartNotExistsException extends RuntimeException {
    public DatamartNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
