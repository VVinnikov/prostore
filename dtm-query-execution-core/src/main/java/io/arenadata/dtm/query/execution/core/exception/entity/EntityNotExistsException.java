package io.arenadata.dtm.query.execution.core.exception.entity;

public class EntityNotExistsException extends RuntimeException {

    public EntityNotExistsException(String entity) {
        super(String.format("Entity [%s] not exists!", entity));
    }

    public EntityNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
