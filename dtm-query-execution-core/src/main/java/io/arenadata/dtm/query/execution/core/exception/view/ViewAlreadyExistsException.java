package io.arenadata.dtm.query.execution.core.exception.view;

import io.arenadata.dtm.query.execution.core.exception.DtmException;

public class ViewAlreadyExistsException extends DtmException {

    public ViewAlreadyExistsException(String table) {
        super(String.format("Entity %s already exists", table));
    }

    public ViewAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
