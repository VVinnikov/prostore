package io.arenadata.dtm.query.execution.core.dao.exception.entity;

public class EntityAlreadyExistsException extends RuntimeException {
    public EntityAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
