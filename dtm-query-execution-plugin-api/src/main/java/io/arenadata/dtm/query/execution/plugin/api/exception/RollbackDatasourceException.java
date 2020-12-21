package io.arenadata.dtm.query.execution.plugin.api.exception;

public class RollbackDatasourceException extends DataSourceException {
    public RollbackDatasourceException(String s) {
        super(s);
    }

    public RollbackDatasourceException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
