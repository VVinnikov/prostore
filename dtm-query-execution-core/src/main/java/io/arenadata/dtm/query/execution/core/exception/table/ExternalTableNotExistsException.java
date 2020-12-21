package io.arenadata.dtm.query.execution.core.exception.table;

import io.arenadata.dtm.common.exception.DtmException;

public class ExternalTableNotExistsException extends DtmException {

    public ExternalTableNotExistsException(String entity) {
        super(String.format("External table %s does not exist", entity));
    }

    public ExternalTableNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
