package io.arenadata.dtm.query.execution.plugin.api.exception;

public class MpprDatasourceException extends DataSourceException {
    public MpprDatasourceException(String s) {
        super(s);
    }

    public MpprDatasourceException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
