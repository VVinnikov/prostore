package ru.ibs.dtm.query.execution.core.dao.exception.delta;

public class DeltaWriteOpNotFoundException extends DeltaException {
    private static final String MESSAGE = "write op not found";

    public DeltaWriteOpNotFoundException() {
        super(MESSAGE);
    }

    public DeltaWriteOpNotFoundException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
