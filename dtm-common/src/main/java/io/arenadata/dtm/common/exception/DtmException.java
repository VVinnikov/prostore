package io.arenadata.dtm.common.exception;

public class DtmException extends RuntimeException {

    public DtmException(String message) {
        super(message);
    }

    public DtmException(Throwable throwable) {
        super(throwable);
    }

    public DtmException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
