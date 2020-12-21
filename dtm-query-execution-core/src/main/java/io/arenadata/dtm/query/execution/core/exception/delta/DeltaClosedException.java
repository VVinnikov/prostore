package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaClosedException extends DeltaException {
    private static final String MESSAGE = "Delta closed";

    public DeltaClosedException() {
        super(MESSAGE);
    }
}
