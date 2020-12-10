package io.arenadata.dtm.query.execution.core.exception.entity;

import io.arenadata.dtm.query.execution.core.exception.DtmException;

public class EntityAlreadyExistsException extends DtmException {
    public EntityAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
