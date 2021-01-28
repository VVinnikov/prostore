package io.arenadata.dtm.query.execution.core.exception.view;

import io.arenadata.dtm.common.exception.DtmException;

public class ViewNotExistsException extends DtmException {

    public ViewNotExistsException(String view) {
        super(String.format("View %s does not exist", view));
    }

    public ViewNotExistsException(String schemaName, String tableName) {
        super(String.format("View %s.%s does not exist", schemaName, tableName));
    }

    public ViewNotExistsException(String message, Throwable cause) {
        super(message, cause);
    }

}
