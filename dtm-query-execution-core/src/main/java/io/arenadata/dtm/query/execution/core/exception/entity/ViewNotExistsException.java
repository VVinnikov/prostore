package io.arenadata.dtm.query.execution.core.exception.entity;

import io.arenadata.dtm.query.execution.core.exception.DtmException;

public class ViewNotExistsException extends DtmException {

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
