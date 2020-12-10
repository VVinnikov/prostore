package io.arenadata.dtm.query.execution.core.exception.delta;

public class InvalidDeltaDateException extends DeltaException {
    public InvalidDeltaDateException() {
        super("invalid delta date");
    }
}
