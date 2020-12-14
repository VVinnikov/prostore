package io.arenadata.dtm.query.execution.plugin.api.exception;

public class DataSourceException extends RuntimeException {

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
