package ru.ibs.dtm.query.execution.core.dao.exception;

public class ViewNotExistsException extends RuntimeException {

    public ViewNotExistsException(String view) {
        super(String.format("View [%s] not exists!", view));
    }

    public ViewNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public static <T> T throwError(String name) {
        throw new ViewNotExistsException(name);
    }
}
