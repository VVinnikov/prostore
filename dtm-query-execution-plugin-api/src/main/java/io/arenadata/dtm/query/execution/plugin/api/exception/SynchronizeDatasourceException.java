package io.arenadata.dtm.query.execution.plugin.api.exception;

public class SynchronizeDatasourceException extends DataSourceException {
    public SynchronizeDatasourceException(String s) {
        super(s);
    }

    public SynchronizeDatasourceException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
