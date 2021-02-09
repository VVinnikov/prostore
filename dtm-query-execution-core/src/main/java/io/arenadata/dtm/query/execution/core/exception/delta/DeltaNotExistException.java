package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaNotExistException extends DeltaException {
    private static final String MESSAGE = "Delta not exist";

    public DeltaNotExistException() {
        super(MESSAGE);
    }
}
