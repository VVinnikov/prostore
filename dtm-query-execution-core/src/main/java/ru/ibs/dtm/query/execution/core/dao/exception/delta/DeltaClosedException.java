package ru.ibs.dtm.query.execution.core.dao.exception.delta;

public class DeltaClosedException extends DeltaException {
    private static final String MESSAGE = "Delta closed";

    public DeltaClosedException() {
        super(MESSAGE);
    }
}
