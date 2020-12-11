package io.arenadata.dtm.query.execution.core.exception.delta;

public class DeltaUnableSetDateTimeException extends DeltaException {
    public DeltaUnableSetDateTimeException(String dateTime) {
        super(String.format("Unable to set the date-time %s preceding the actual delta", dateTime));
    }
}
