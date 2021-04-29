package io.arenadata.dtm.query.execution.core.delta.exception;

public class DeltaUnableSetDateTimeException extends DeltaException {
    public DeltaUnableSetDateTimeException(String dateTime, String actualDateTime) {
        super(String.format("Unable to set the date-time %s preceding the actual delta %s", dateTime, actualDateTime));
    }
}
