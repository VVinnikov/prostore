package io.arenadata.dtm.query.execution.core.dao.exception.delta;

public class DeltaNotStartedException extends DeltaException {
    private static final String MESSAGE = "Delta not started";

    public DeltaNotStartedException() {
        super(MESSAGE);
    }
}
