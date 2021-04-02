package io.arenadata.dtm.query.execution.core.check.exception;

import io.arenadata.dtm.common.exception.DtmException;

public class CheckSumException extends DtmException {

    public CheckSumException(String tableName) {
        super(String.format("Consistency breach detected for %s", tableName));
    }
}
