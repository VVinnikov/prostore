package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaAlreadyStartedException extends DeltaException {
    private static final String MESSAGE = "Delta already started";

    public DeltaAlreadyStartedException() {
        super(MESSAGE);
    }

    public DeltaAlreadyStartedException(String message) {
        super(message);
    }

    public DeltaAlreadyStartedException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeltaAlreadyStartedException(Throwable error) {
        super(MESSAGE, error);
    }
}
