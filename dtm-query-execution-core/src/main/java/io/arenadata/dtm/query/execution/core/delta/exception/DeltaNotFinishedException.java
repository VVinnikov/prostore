package io.arenadata.dtm.query.execution.core.delta.exception;

public class DeltaNotFinishedException extends DeltaException {
    private static final String MESSAGE = "not finished write operation exist";

    public DeltaNotFinishedException() {
        super(MESSAGE);
    }

    public DeltaNotFinishedException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
