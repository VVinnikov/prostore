package io.arenadata.dtm.query.execution.core.dao.exception.delta;

public class DeltaHotNotStartedException extends DeltaException {
    public DeltaHotNotStartedException() {
        super("Delta hot not started");
    }
}
