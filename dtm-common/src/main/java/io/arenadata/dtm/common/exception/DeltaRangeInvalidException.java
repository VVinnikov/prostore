package io.arenadata.dtm.common.exception;

public class DeltaRangeInvalidException extends DtmException {

    public DeltaRangeInvalidException() {
        super("DeltaRangeInvalid exception");
    }

    public DeltaRangeInvalidException(String message) {
        super(message);
    }

    public DeltaRangeInvalidException(String message, Throwable cause) {
        super(message, cause);
    }
}
