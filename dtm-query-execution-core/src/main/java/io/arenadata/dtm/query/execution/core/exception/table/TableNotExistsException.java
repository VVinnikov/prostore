package io.arenadata.dtm.query.execution.core.exception.table;

import io.arenadata.dtm.common.exception.DtmException;

public class TableNotExistsException extends DtmException {

    public TableNotExistsException(String entity) {
        super(String.format("Table %s does not exist", entity));
    }

    public TableNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
