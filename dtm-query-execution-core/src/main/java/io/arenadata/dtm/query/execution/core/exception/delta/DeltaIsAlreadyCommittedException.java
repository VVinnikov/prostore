package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaIsAlreadyCommittedException extends DeltaException {
    private static final String MESSAGE = "Delta is already commited";

    public DeltaIsAlreadyCommittedException() {
        super(MESSAGE);
    }
}
