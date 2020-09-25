package ru.ibs.dtm.query.execution.core.dao.exception;

public class DatamartNotExistsException extends RuntimeException {

    public DatamartNotExistsException(String datamart) {
        super(String.format("Datamart [%s] not exists!", datamart));
    }

    public DatamartNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
