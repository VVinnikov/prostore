package io.arenadata.dtm.query.execution.core.query.exception;

import io.arenadata.dtm.common.exception.DtmException;

public class PreparedStatementNotFoundException extends DtmException {

    public PreparedStatementNotFoundException() {
        super("Prepared statement not found");
    }

}
