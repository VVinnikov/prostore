package io.arenadata.dtm.query.execution.core.exception;

public class DtmException extends RuntimeException {

    public DtmException(String message) {
        super(message);
    }

    public DtmException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
