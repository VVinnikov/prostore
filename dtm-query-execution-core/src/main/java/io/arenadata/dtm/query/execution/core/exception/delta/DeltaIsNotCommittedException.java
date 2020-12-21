package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaIsNotCommittedException extends DeltaException {
    private static final String MESSAGE = "Delta %s is not committed";

    public DeltaIsNotCommittedException() {
        super(MESSAGE);
    }

    public DeltaIsNotCommittedException(String deltaHot) {
        super(String.format(MESSAGE, deltaHot));
    }

    public DeltaIsNotCommittedException(String deltaHot, Throwable error) {
        super(String.format(MESSAGE, deltaHot), error);
    }
}
