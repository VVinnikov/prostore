package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaNumIsNotNextToActualException extends DeltaException {

    public DeltaNumIsNotNextToActualException(String deltaNum) {
        super(String.format("The delta number %s is not next to an actual delta", deltaNum));
    }
}
