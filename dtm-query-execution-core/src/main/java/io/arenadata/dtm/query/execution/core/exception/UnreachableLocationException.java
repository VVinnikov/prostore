package io.arenadata.dtm.query.execution.core.exception;

import io.arenadata.dtm.common.exception.DtmException;

public class UnreachableLocationException extends DtmException {

    private static final String MESSAGE = "Location %s is unreachable";

    public UnreachableLocationException(String location) {
        super(String.format(MESSAGE, location));
    }

    public UnreachableLocationException(String location, Throwable throwable) {
        super(String.format(MESSAGE, location), throwable);
    }
}
