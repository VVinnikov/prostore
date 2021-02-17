package io.arenadata.dtm.query.execution.core.exception.view;

import io.arenadata.dtm.common.exception.DtmException;

public class DuplicateColumnNameNotAllowedException extends DtmException {

    public DuplicateColumnNameNotAllowedException() {
        super("Duplicate column names are not allowed");
    }
}
