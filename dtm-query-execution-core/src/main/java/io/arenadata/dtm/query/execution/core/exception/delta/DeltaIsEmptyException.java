package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaIsEmptyException extends DeltaException {

    public DeltaIsEmptyException(long deltaNum) {
        super(String.format("Delta %d is empty", deltaNum));
    }
}
