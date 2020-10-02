package ru.ibs.dtm.query.execution.core.dao.exception.delta;

public class DeltaNotFinishedException extends DeltaException {
    private static final String MESSAGE = "not finished write operation exist";

    public DeltaNotFinishedException() {
        super(MESSAGE);
    }

    public DeltaNotFinishedException(Throwable cause) {
        super(MESSAGE, cause);
    }
}
