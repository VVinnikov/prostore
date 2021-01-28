package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaNotFoundException extends DeltaException {
    private static final String MESSAGE = "Delta not found";

    public DeltaNotFoundException() {
        super(MESSAGE);
    }

    public DeltaNotFoundException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
