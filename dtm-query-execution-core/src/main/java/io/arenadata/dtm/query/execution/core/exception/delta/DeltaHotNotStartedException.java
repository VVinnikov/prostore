package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaHotNotStartedException extends DeltaException {
    public DeltaHotNotStartedException() {
        super("Delta hot not started");
    }
}
