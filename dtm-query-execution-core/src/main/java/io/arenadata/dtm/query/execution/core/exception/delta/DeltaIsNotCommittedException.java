package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaIsNotCommittedException extends DeltaException {
    private static final String MESSAGE = "Delta %s is not committed";
    private static final String MESSAGE_WITHOUT_DELTA_NUM = "Current delta hot is not committed";

    public DeltaIsNotCommittedException() {
        super(MESSAGE);
    }

    public DeltaIsNotCommittedException(String deltaHot) {
        super(String.format(MESSAGE, deltaHot));
    }

    public DeltaIsNotCommittedException(String deltaHot, Throwable error) {
        super(String.format(MESSAGE, deltaHot), error);
    }

    public DeltaIsNotCommittedException(Throwable error) {
        super(MESSAGE_WITHOUT_DELTA_NUM, error);
    }
}
