package io.arenadata.dtm.query.execution.core.exception.view;

import io.arenadata.dtm.query.execution.core.exception.DtmException;

public class ViewNotExistsException extends DtmException {

    public ViewNotExistsException(String view) {
        super(String.format("View %s does not exist", view));
    }

    public ViewNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }

}
