package io.arenadata.dtm.query.execution.core.base.exception.entity;

import io.arenadata.dtm.common.exception.DtmException;

public class EntityNotExistsException extends DtmException {

    public EntityNotExistsException(String entityName) {
        super(String.format("Entity %s does not exist", entityName));
    }

    public EntityNotExistsException(String schemaName, String entityName) {
        super(String.format("Entity %s.%s does not exist", schemaName, entityName));
    }

}
