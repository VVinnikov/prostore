package io.arenadata.dtm.query.execution.core.base.exception.table;

import io.arenadata.dtm.common.exception.DtmException;

public class ColumnNotExistsException extends DtmException {

    public ColumnNotExistsException(String entity) {
        super(String.format("Column %s does not exist", entity));
    }

    public ColumnNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
