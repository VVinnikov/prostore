package io.arenadata.dtm.query.execution.core.delta.exception;

public class DeltaHotNotStartedException extends DeltaException {
    public DeltaHotNotStartedException() {
        super("Delta hot not started");
    }
}
