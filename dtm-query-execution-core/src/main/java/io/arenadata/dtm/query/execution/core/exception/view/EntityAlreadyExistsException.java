package io.arenadata.dtm.query.execution.core.exception.view;

import io.arenadata.dtm.common.exception.DtmException;

public class EntityAlreadyExistsException extends DtmException {

    public EntityAlreadyExistsException(String table) {
        super(String.format("Entity %s already exists", table));
    }

    public EntityAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}