package ru.ibs.dtm.query.execution.core.dao.exception.delta;

public class DeltaNotExistException extends DeltaException {
    private static final String MESSAGE = "Delta not exist";

    public DeltaNotExistException() {
        super(MESSAGE);
    }
}
