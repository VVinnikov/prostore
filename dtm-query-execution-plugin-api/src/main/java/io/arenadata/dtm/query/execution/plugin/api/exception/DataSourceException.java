package io.arenadata.dtm.query.execution.plugin.api.exception;

import io.arenadata.dtm.common.exception.DtmException;

public class DataSourceException extends DtmException {

    public DataSourceException(String s) {
        super(s);
    }

    public DataSourceException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DataSourceException(Throwable throwable) {
        super(throwable);
    }
}
