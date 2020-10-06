package ru.ibs.dtm.query.execution.core.dao.exception.entity;

public class EntityNotExistsException extends RuntimeException {

    public EntityNotExistsException(String entity) {
        super(String.format("Entity [%s] not exists!", entity));
    }

    public EntityNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
