package io.arenadata.dtm.query.execution.core.delta.exception;

public class DeltaAlreadyCommitedException extends DeltaException {
    private static final String MESSAGE = "Delta already commited";

    public DeltaAlreadyCommitedException() {
        super(MESSAGE);
    }

    public DeltaAlreadyCommitedException(Throwable error) {
        super(MESSAGE, error);
    }
}
