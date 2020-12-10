package io.arenadata.dtm.query.execution.core.exception.entity;

import io.arenadata.dtm.query.execution.core.exception.DtmException;

public class TableNotExistsException extends DtmException {

    public TableNotExistsException(String table) {
        super(String.format("Table [%s] not exists!", table));
    }

    public TableNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
