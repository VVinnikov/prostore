package io.arenadata.dtm.query.execution.core.exception.table;

import io.arenadata.dtm.common.exception.DtmException;

public class TableAlreadyExistsException extends DtmException {

    public TableAlreadyExistsException(String table) {
        super(String.format("Table %s already exists", table));
    }

    public TableAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
