package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaAlreadyIsRollingBackException extends DeltaException {
    private static final String MESSAGE = "Delta already is rolling back";

    public DeltaAlreadyIsRollingBackException() {
        super(MESSAGE);
    }

    public DeltaAlreadyIsRollingBackException(String message) {
        super(message);
    }

    public DeltaAlreadyIsRollingBackException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeltaAlreadyIsRollingBackException(Throwable error) {
        super(MESSAGE, error);
    }
}
