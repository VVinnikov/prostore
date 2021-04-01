package io.arenadata.dtm.query.execution.core.exception.delta;

public class NegativeDeltaNumberException extends DeltaException {
    private static final String MESSAGE = "Negative delta number is unexpected";
    public NegativeDeltaNumberException() {
        super(MESSAGE);
    }

}
