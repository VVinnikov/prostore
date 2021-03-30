package io.arenadata.dtm.query.execution.core.exception.datamart;

import io.arenadata.dtm.common.exception.DtmException;

public class DatamartNotExistsException extends DtmException {

    public DatamartNotExistsException(String datamart) {
        super(String.format("Database %s does not exist", datamart));
    }

    public DatamartNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
