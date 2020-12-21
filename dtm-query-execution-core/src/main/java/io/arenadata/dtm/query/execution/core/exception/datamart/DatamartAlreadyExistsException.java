package io.arenadata.dtm.query.execution.core.exception.datamart;

import io.arenadata.dtm.common.exception.DtmException;

public class DatamartAlreadyExistsException extends DtmException {

    public DatamartAlreadyExistsException(String datamart) {
        super(String.format("Database %s already exists", datamart));
    }

    public DatamartAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
