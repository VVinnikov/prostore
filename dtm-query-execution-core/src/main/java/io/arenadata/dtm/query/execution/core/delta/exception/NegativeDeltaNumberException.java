package io.arenadata.dtm.query.execution.core.delta.exception;

public class NegativeDeltaNumberException extends DeltaException {
    private static final String MESSAGE = "Negative delta number is unexpected";
    public NegativeDeltaNumberException() {
        super(MESSAGE);
    }

}
