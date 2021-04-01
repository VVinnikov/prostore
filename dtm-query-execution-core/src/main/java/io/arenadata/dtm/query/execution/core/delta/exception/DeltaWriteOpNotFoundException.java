package io.arenadata.dtm.query.execution.core.delta.exception;

public class DeltaWriteOpNotFoundException extends DeltaException {
    private static final String MESSAGE = "write op not found";

    public DeltaWriteOpNotFoundException() {
        super(MESSAGE);
    }

    public DeltaWriteOpNotFoundException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
