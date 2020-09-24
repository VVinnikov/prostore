package ru.ibs.dtm.query.execution.core.dao.exception;

public class EntityNotExistsException extends RuntimeException {
    public EntityNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
