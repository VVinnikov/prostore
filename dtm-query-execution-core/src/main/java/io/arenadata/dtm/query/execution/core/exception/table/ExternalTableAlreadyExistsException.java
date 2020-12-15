package io.arenadata.dtm.query.execution.core.exception.table;

import io.arenadata.dtm.common.exception.DtmException;

public class ExternalTableAlreadyExistsException extends DtmException {

    public ExternalTableAlreadyExistsException(String table) {
        super(String.format("External table %s already exists", table));
    }

    public ExternalTableAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
