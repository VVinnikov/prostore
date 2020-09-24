package ru.ibs.dtm.query.execution.core.dao.exception;

public class DatamartAlreadyExistsException extends RuntimeException {
    public DatamartAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
