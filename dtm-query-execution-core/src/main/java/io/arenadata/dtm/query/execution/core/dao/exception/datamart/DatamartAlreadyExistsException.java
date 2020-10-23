package io.arenadata.dtm.query.execution.core.dao.exception.datamart;

public class DatamartAlreadyExistsException extends RuntimeException {
    public DatamartAlreadyExistsException(String datamart) {
        super(String.format("Datamart [%s] already exists", datamart));
    }

    public DatamartAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
