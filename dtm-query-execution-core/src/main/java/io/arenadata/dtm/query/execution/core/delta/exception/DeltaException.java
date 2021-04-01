package io.arenadata.dtm.query.execution.core.delta.exception;

import io.arenadata.dtm.common.exception.DtmException;

public class DeltaException extends DtmException {

    public DeltaException(String message) {
        super(message);
    }

    public DeltaException(String message, Throwable cause) {
        super(message + ": " + cause.getMessage(), cause);
    }
}
