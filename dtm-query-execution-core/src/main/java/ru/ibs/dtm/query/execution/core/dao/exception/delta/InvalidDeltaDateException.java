package ru.ibs.dtm.query.execution.core.dao.exception.delta;

public class InvalidDeltaDateException extends DeltaException {
    public InvalidDeltaDateException() {
        super("invalid delta date");
    }
}
