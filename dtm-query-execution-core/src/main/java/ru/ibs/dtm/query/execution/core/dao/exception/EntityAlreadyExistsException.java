package ru.ibs.dtm.query.execution.core.dao.exception;

public class EntityAlreadyExistsException extends RuntimeException {
    public EntityAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
