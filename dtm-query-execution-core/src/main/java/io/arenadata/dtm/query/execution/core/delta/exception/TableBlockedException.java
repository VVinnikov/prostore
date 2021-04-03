package io.arenadata.dtm.query.execution.core.delta.exception;

public class TableBlockedException extends DeltaException {
    private static final String MESSAGE = "Table[%s] blocked";

    public TableBlockedException(String tableName, Throwable cause) {
        super(String.format(MESSAGE, tableName), cause);
    }
}
