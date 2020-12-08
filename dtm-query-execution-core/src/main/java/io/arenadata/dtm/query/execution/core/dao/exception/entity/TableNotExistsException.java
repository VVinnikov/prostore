package io.arenadata.dtm.query.execution.core.dao.exception.entity;

public class TableNotExistsException extends RuntimeException {

    public TableNotExistsException(String table) {
        super(String.format("Table [%s] not exists!", table));
    }

    public TableNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
