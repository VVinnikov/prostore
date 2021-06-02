package io.arenadata.dtm.query.execution.core.base.exception.entity;

import io.arenadata.dtm.common.exception.DtmException;

public class EntityAlreadyExistsException extends DtmException {

    public EntityAlreadyExistsException(String entity) {
        super(String.format("Entity %s already exists", entity));
    }

    public EntityAlreadyExistsException(String entity, Throwable cause) {
        super(String.format("Entity %s already exists", entity), cause);
    }
}
